# OpenCode PWA Monitor

Espelho do OpenCode no celular: monitore as 5 instâncias rodando no PC em tempo real (dashboard de atividade ao vivo, badges ativo/inativo), abra qualquer sessão, envie mensagens e crie sessões novas direto do navegador.

## Funcionalidades

- **Dashboard ao vivo**: sessões ativas com streaming SSE (pensando → executando tool → escrevendo resposta → concluído)
- **Sessões**: listar, abrir, criar e trocar de sessão
- **Modelo por sessão**: seletor com 3 providers / 54 modelos (opencode, 9router, kiro)
- **Badges de status**: ● Ativo / ○ inativo (com minutos) / ✅ Concluído
- **Painel de Logs** (frontend): anel dos últimos 300 eventos (app.init, api.*, sse.*, session.*, message.*)
- **Logs estruturados** (servidor): JSON em `logs/server.log` com níveis debug/info/warn/error e duração de cada request
- **PWA**: instalável via manifest.json

## Arquitetura

```
┌─────────────┐   HTTP/SSE    ┌────────────────┐   HTTP    ┌─────────────────┐
│  Navegador  │ ────────────► │  server.js     │ ────────► │  opencode serve │
│ (PWA, 4335) │               │  (proxy 4335)  │           │  (API, 4333)    │
└─────────────┘               └────────────────┘           └─────────────────┘
```

- `server.js` — proxy Node zero-dependência (4335): roteia `/project`, `/session`, `/api/`, `/config` para a API do opencode (4333), com SSE streaming e logs estruturados.
- `app.js` — lógica Alpine.js (dashboard, sessões, chat, SSE, otimismo, logs).
- `index.html` + `style.css` — UI dark, mobile-first.
- `manifest.json` — instalação PWA.

## Setup

```bash
# 1. Auth (usuário/senha Basic para o proxy) — via env ou auth.local.json
export OPENCODE_USERNAME=seu_usuario
export OPENCODE_PASSWORD=sua_senha
# ou: crie auth.local.json {"username":"...","password":"..."} (gitignored)

# 2. Garanta que a API do opencode esteja no ar
opencode serve --hostname 0.0.0.0 --port 4333

# 3. Rode o proxy
node server.js
# → PWA + proxy em http://0.0.0.0:4335

# Opcional: nível de log e debug
LOG_LEVEL=debug PROXY_DEBUG=1 node server.js
```

Acesse `http://<ip-do-pc>:4335` no navegador do celular (mesma rede) e instale como app.

## Logs

- **Servidor**: `logs/server.log` — JSON por linha: `{"ts":..., "level":"info", "event":"proxy.response", "data":{...}}`
- **Frontend**: botão **📋 Logs** no topo — anel dos últimos 300 eventos, com nível e timestamp.
- `LOG_LEVEL` controla o filtro do servidor (debug|info|warn|error); `PROXY_DEBUG` espelha no console.

## Adicionar um node (ex: VPS)

O proxy agrega vários nodes `opencode serve`. O node `main` é o upstream local
(`API_HOST`/`API_PORT`); nodes extras entram via env `NODES_<ID>_HOST/PORT/NAME/COLOR/SSH`
no unit systemd. Use o script `scripts/add-node.sh` — ele valida a conectividade
com health check real **antes** de aplicar, escreve um drop-in systemd, reinicia
o serviço e confere em `/api/nodes` (com rollback em falha):

```bash
# 1. No node (VPS): gere um token e suba o serve
opencode serve --hostname 0.0.0.0 --port 4096

# 2. No PC do proxy: adicione o node
./scripts/add-node.sh --id vps --host <ip-do-vps> --port 4096 \
  --name VPS --color '#58a6ff' --ssh user@<ip-do-vps>

# 3. Confira
curl http://127.0.0.1:4335/api/nodes   # vps deve aparecer reachable:true
```

- `--id` (obrigatório): `[a-z0-9]+` (ex: `vps`, `lubuntu`).
- `--host` (obrigatório): IP/hostname do node.
- `--port` (default `4333`), `--name` (default = id), `--color` (hex `#rrggbb`), `--ssh` (opcional, p/ telemetria remota).
- Se o node não responder ao health check, o script **aborta sem alterar nada**.
- Para remover: apague o drop-in `~/.config/systemd/user/opencode-proxy.service.d/nodes.conf` e rode `systemctl --user daemon-reload && systemctl --user restart opencode-proxy.service`.

## Segurança

- Credenciais **não** ficam no código: env ou `auth.local.json` (gitignored).
- Recomendado rodar atrás de rede confiável ou VPN (Tailscale).

### HTTPS (opcional)

Por padrão o proxy roda em HTTP puro. Para habilitar TLS, aponte `HTTPS_CERT` e
`HTTPS_KEY` para os arquivos do certificado:

```bash
# Tailscale (self-signed basta — o app confia na rede privada)
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /etc/opencode/key.pem -out /etc/opencode/cert.pem \
  -days 365 -subj "/CN=<ip-do-pc>"

# VPS público: use Let's Encrypt (certbot) e aponte para os arquivos emitidos

# systemd unit (drop-in) ou env:
HTTPS_CERT=/etc/opencode/cert.pem HTTPS_KEY=/etc/opencode/key.pem node server.js
# → PWA + proxy em https://0.0.0.0:4335
```

No app mobile, use `https://<host>:4335` no campo Endereço (ou
`EXPO_PUBLIC_OPENCODE_HOST`). Com self-signed, o Android pode exigir aceitar o
certificado na primeira conexão.

### Rotação de credenciais

1. **Via env (systemd)**: edite `OPENCODE_USERNAME`/`OPENCODE_PASSWORD` no unit
   e rode `systemctl --user daemon-reload && systemctl --user restart opencode-proxy.service`.
   A credencial antiga passa a retornar **401** imediatamente.
2. **Via arquivo (a quente, sem restart)**: edite `auth.local.json` e envie
   `kill -HUP <pid>` (ou `systemctl --user kill -s HUP opencode-proxy.service`).
   O proxy recarrega o arquivo e troca a credencial em < 1s.

Após rotacionar, atualize a senha no app (Ajustes → Senha) e salve.

## Licença

MIT
