'use strict';
/*
 * Servidor online do Roleplay Imperial — até 120 jogadores simultâneos.
 * Protocolo compatível com o cliente do jogo (WebSocket em /ws, JSON).
 *
 * Rodar:  npm install && npm start      (porta padrão 3000)
 * Variáveis: PORT, MAX_PLAYERS (120), DATA_FILE, TRUST_PROXY=1, MAX_PER_IP (10)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const MAX = +process.env.MAX_PLAYERS || 120;
const MAX_PER_IP = +process.env.MAX_PER_IP || 10;
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'accounts.json');
const PUB = path.join(__dirname, 'public');

const NEAR2 = 250 * 250;   // posições enviadas a 10 Hz só para quem está a até 250 m
const VOICE2 = 100 * 100;  // voz só chega a quem está a até 100 m
const NAME_RE = /^[\p{L}\p{N}_.-]{3,14}$/u;

/* ---------- contas (arquivo JSON, gravação atômica e com atraso) ---------- */
let accounts = {};
try { accounts = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { accounts = {}; }
let saveTimer = null;
function writeAccounts() {
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(accounts));
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) { console.error('Erro ao salvar contas:', e.message); }
}
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; writeAccounts(); }, 3000);
}
const scrypt = (p, salt) => new Promise((res, rej) =>
  crypto.scrypt(p, salt, 32, (e, k) => (e ? rej(e) : res(k))));

/* ---------- estado ---------- */
const sessions = new Map();  // id -> cliente autenticado
const byName = new Map();    // nome minúsculo -> cliente
const ipConns = new Map();   // ip -> nº de conexões
const ipFails = new Map();   // ip -> [timestamps de falhas de login]
let nextId = 1;

function send(c, o, droppable) {
  const ws = c.ws;
  if (ws.readyState !== 1) return;
  if (droppable && ws.bufferedAmount > 512 * 1024) return; // cliente lento: descarta pos/voz
  ws.send(JSON.stringify(o));
}
function broadcast(o, except, droppable) {
  const s = JSON.stringify(o);
  for (const c of sessions.values()) {
    if (c === except || c.ws.readyState !== 1) continue;
    if (droppable && c.ws.bufferedAmount > 512 * 1024) continue;
    c.ws.send(s);
  }
}
const pub = c => ({ id: c.id, n: c.name, x: c.x, z: c.z, h: c.h, f: c.f });

function throttled(ip) {
  const now = Date.now();
  const a = (ipFails.get(ip) || []).filter(t => now - t < 60000);
  ipFails.set(ip, a);
  return a.length >= 8;
}
function fail(c, m) {
  const a = ipFails.get(c.ip) || [];
  a.push(Date.now());
  ipFails.set(c.ip, a);
  send(c, { t: 'err', m });
}

/* ---------- login / registro ---------- */
async function auth(c, d, reg) {
  if (c.busy || c.auth) return;
  c.busy = true;
  try {
    if (throttled(c.ip)) return send(c, { t: 'err', m: 'Muitas tentativas. Aguarde um minuto.' });
    const u = String(d.u == null ? '' : d.u).trim();
    const p = String(d.p == null ? '' : d.p);
    if (!NAME_RE.test(u)) return send(c, { t: 'err', m: 'Usuário: 3 a 14 caracteres (letras, números, _ . -)' });
    if (p.length < 4 || p.length > 64) return send(c, { t: 'err', m: 'Senha: de 4 a 64 caracteres' });
    const key = u.toLowerCase();
    let acc = accounts[key];

    if (reg) {
      if (acc) return send(c, { t: 'err', m: 'Esse usuário já existe' });
      const salt = crypto.randomBytes(16);
      const hash = await scrypt(p, salt);
      if (accounts[key]) return send(c, { t: 'err', m: 'Esse usuário já existe' });
      acc = accounts[key] = {
        name: u, salt: salt.toString('hex'), hash: hash.toString('hex'),
        money: 0, deliv: 0, hasGun: false, x: '', created: Date.now()
      };
      scheduleSave();
    } else {
      if (!acc) return fail(c, 'Usuário ou senha incorretos');
      const hash = await scrypt(p, Buffer.from(acc.salt, 'hex'));
      const ok = crypto.timingSafeEqual(hash, Buffer.from(acc.hash, 'hex'));
      if (!ok) return fail(c, 'Usuário ou senha incorretos');
    }

    // checagens finais (sem await depois daqui, então não há corrida)
    if (c.ws.readyState !== 1) return;
    if (byName.has(key)) return send(c, { t: 'err', m: 'Essa conta já está online' });
    if (sessions.size >= MAX) return send(c, { t: 'err', m: 'Servidor lotado (' + MAX + '/' + MAX + ')' });

    c.auth = true; c.acc = acc; c.name = acc.name;
    c.x = c.z = c.h = 0; c.f = 0; c.s = 0; c.v = ''; c.dirty = false;
    const others = [...sessions.values()].map(pub);
    sessions.set(c.id, c);
    byName.set(key, c);
    send(c, {
      t: 'ok', n: acc.name, money: acc.money || 0, deliv: acc.deliv || 0,
      hasGun: !!acc.hasGun, x: acc.x || '', count: sessions.size, players: others
    });
    broadcast(Object.assign({ t: 'join' }, pub(c)), c);
    broadcast({ t: 'count', n: sessions.size });
  } catch (e) {
    console.error('auth:', e.message);
    send(c, { t: 'err', m: 'Erro no servidor' });
  } finally { c.busy = false; }
}

/* ---------- mensagens do jogo ---------- */
function allow(c, cost) {                       // balde de fichas: ~120 msgs/s, rajada 200
  const now = Date.now();
  c.tok = Math.min(200, c.tok + (now - c.tokT) * 0.12);
  c.tokT = now;
  if (c.tok < cost) return false;
  c.tok -= cost;
  return true;
}

function onMessage(c, raw) {
  let d;
  try { d = JSON.parse(raw); } catch (e) { return; }
  if (!d || typeof d !== 'object' || !allow(c, 1)) return;

  if (!c.auth) {
    if (d.t === 'login') auth(c, d, false);
    else if (d.t === 'reg') auth(c, d, true);
    return;
  }

  switch (d.t) {
    case 'pos': {
      const x = +d.x, z = +d.z, h = +d.h, s = +d.s;
      if (!Number.isFinite(x) || !Number.isFinite(z) || !Number.isFinite(h) || !Number.isFinite(s)) return;
      if (Math.abs(x) > 50000 || Math.abs(z) > 50000) return;
      c.x = x; c.z = z; c.h = h; c.f = d.f ? 1 : 0;
      c.v = (typeof d.v === 'string' && /^[a-z0-9:.-]{0,24}$/.test(d.v)) ? d.v : '';
      c.s = Math.max(0, Math.min(60, s));
      c.dirty = true;
      break;
    }
    case 'chat': {
      const m = String(d.m == null ? '' : d.m).slice(0, 300);
      if (!m.trim()) return;
      const now = Date.now();
      c.chatT = (c.chatT || []).filter(t => now - t < 5000);
      if (c.chatT.length >= 8) return;          // máx. 8 mensagens a cada 5 s
      c.chatT.push(now);
      broadcast({ t: 'chat', id: c.id, n: c.name, m });
      break;
    }
    case 'save': {
      const a = c.acc, money = Math.floor(+d.money), deliv = Math.floor(+d.deliv);
      if (Number.isFinite(money) && money >= 0 && money <= 1e12) a.money = money;
      if (Number.isFinite(deliv) && deliv >= 0 && deliv <= 1e12) a.deliv = deliv;
      a.hasGun = !!d.hasGun;
      if (typeof d.x === 'string' && d.x.length <= 4000) a.x = d.x;
      scheduleSave();
      break;
    }
    case 'inv': {
      const o = byName.get(String(d.to || '').toLowerCase());
      if (o && o !== c) send(o, { t: 'inv', from: c.name, o: String(d.o).slice(0, 8) });
      break;
    }
    case 'invr': {
      const o = byName.get(String(d.to || '').toLowerCase());
      if (o && o !== c) send(o, { t: 'invr', from: c.name, o: String(d.o).slice(0, 8), ok: d.ok ? 1 : 0 });
      break;
    }
    case 'voice': {
      if (typeof d.d !== 'string' || d.d.length >= 4000 || !allow(c, 3)) return;
      const s = JSON.stringify({ t: 'voice', n: c.name, d: d.d });
      for (const o of sessions.values()) {
        if (o === c || o.ws.readyState !== 1 || o.ws.bufferedAmount > 512 * 1024) continue;
        const dx = o.x - c.x, dz = o.z - c.z;
        if (dx * dx + dz * dz <= VOICE2) o.ws.send(s);
      }
      break;
    }
  }
}

/* ---------- posições: lotes a 10 Hz, só os próximos; todos a 1 Hz ---------- */
let tick = 0;
setInterval(() => {
  tick++;
  const full = tick % 10 === 0;
  const list = [...sessions.values()];
  if (list.length > 1) {
    for (const r of list) {
      const p = [];
      for (const o of list) {
        if (o === r || !(o.dirty || full)) continue;
        if (!full) {
          const dx = o.x - r.x, dz = o.z - r.z;
          if (dx * dx + dz * dz > NEAR2) continue;
        }
        p.push([o.id, o.x, o.z, o.h, o.f, o.s, o.v || '']);
      }
      if (p.length) send(r, { t: 'st', p }, true);
    }
  }
  for (const o of list) o.dirty = false;
}, 100);

/* ---------- HTTP (serve o jogo) + WebSocket ---------- */
const MIME = {
  '.html': 'text/html; charset=utf-8', '.json': 'application/json', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.webmanifest': 'application/manifest+json'
};
function serveFile(res, file) {
  fs.readFile(file, (e, buf) => {
    if (e) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600'
    });
    res.end(buf);
  });
}
const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ players: sessions.size, max: MAX }));
  }
  if (url === '/' || url === '/index.html') {
    const a = path.join(PUB, 'index.html'), b = path.join(__dirname, 'index.html');
    return serveFile(res, fs.existsSync(a) ? a : b);
  }
  if (url === '/manifest.json' || url === '/icon-192.png' || url === '/icon-512.png') {
    const rf = path.join(PUB, url);
    return serveFile(res, fs.existsSync(rf) ? rf : path.join(__dirname, url));
  }
  const f = path.normalize(path.join(PUB, url));
  if (!f.startsWith(PUB + path.sep) || !MIME[path.extname(f).toLowerCase()]) {
    res.writeHead(404); return res.end('Not found');
  }
  serveFile(res, f);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024, perMessageDeflate: false });

wss.on('connection', (ws, req) => {
  let ip = req.socket.remoteAddress || '?';
  if (TRUST_PROXY && req.headers['x-forwarded-for']) ip = String(req.headers['x-forwarded-for']).split(',')[0].trim();
  const n = (ipConns.get(ip) || 0) + 1;
  if (n > MAX_PER_IP || wss.clients.size > MAX * 2) { ws.close(1013, 'busy'); return; }
  ipConns.set(ip, n);

  const c = { id: nextId++, ws, ip, auth: false, busy: false, tok: 200, tokT: Date.now(), alive: true };
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => onMessage(c, raw.toString()));
  ws.on('close', () => {
    const k = (ipConns.get(ip) || 1) - 1;
    if (k <= 0) ipConns.delete(ip); else ipConns.set(ip, k);
    if (c.auth) {
      sessions.delete(c.id);
      byName.delete(c.name.toLowerCase());
      broadcast({ t: 'leave', id: c.id });
      broadcast({ t: 'count', n: sessions.size });
      scheduleSave();
    }
  });
  ws.on('error', () => {});
  send(c, { t: 'hello', n: sessions.size, max: MAX });
  setTimeout(() => { if (!c.auth && ws.readyState === 1) ws.close(); }, 30000); // sem login em 30 s
});

setInterval(() => {                              // derruba conexões mortas
  for (const ws of wss.clients) {
    const c = ws;
    if (c.__dead) { ws.terminate(); continue; }
    c.__dead = true;
    ws.once('pong', () => { c.__dead = false; });
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

function shutdown() { writeAccounts(); process.exit(0); }
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PORT, () => console.log('Roleplay Imperial online na porta ' + PORT + ' · máx. ' + MAX + ' jogadores'));
