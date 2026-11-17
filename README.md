# Roleplay Imperial — servidor online (120 jogadores)

1. Instale o Node 18+ e rode: `npm install` e depois `npm start`
2. O jogo abre em `http://SEU-IP:3000` (o `index.html` já está incluído) e conecta sozinho em `/ws`.
3. Limite de jogadores: variável `MAX_PLAYERS` (padrão 120). Contas ficam em `accounts.json`.
4. Para HTTPS/`wss://` (necessário em hospedagem pública), ponha atrás de Nginx/Caddy/Cloudflare e use `TRUST_PROXY=1`.
5. No APK, troque no HTML `window.MR_SERVER=''` por `'wss://seu-dominio.com/ws'`.

Teste de saúde: `/health` mostra jogadores online.
