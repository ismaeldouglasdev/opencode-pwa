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

## Segurança

- Credenciais **não** ficam no código: env ou `auth.local.json` (gitignored).
- Recomendado rodar atrás de rede confiável ou VPN (não há TLS próprio).

## Licença

MIT
