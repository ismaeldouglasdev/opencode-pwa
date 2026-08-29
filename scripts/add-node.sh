#!/usr/bin/env bash
# add-node.sh — adiciona um node ao proxy opencode-pwa (unit systemd user).
#
# Valida a conectividade do node com health check real ANTES de aplicar,
# escreve um drop-in systemd com as vars NODES_<ID>_*, reinicia o serviço
# e confere em /api/nodes. Em qualquer falha, faz rollback do drop-in.
#
# Uso:
#   ./scripts/add-node.sh --id vps --host <ip> --port 4096 [--name VPS] [--color #58a6ff] [--ssh user@host]
#
# Zero dependências (bash + curl + systemctl). pt-BR.

set -euo pipefail

UNIT="opencode-proxy.service"
DROPIN_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/${UNIT}.d"
DROPIN="${DROPIN_DIR}/nodes.conf"
PROXY_URL="${PROXY_URL:-http://127.0.0.1:4335}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-5}"

# ── defaults ────────────────────────────────────────────────────────────
ID=""
HOST=""
PORT="4333"
NAME=""
COLOR="#58a6ff"
SSH=""

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

# ── parse de args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --id)    ID="$2";    shift 2 ;;
    --host)  HOST="$2";  shift 2 ;;
    --port)  PORT="$2";  shift 2 ;;
    --name)  NAME="$2";  shift 2 ;;
    --color) COLOR="$2"; shift 2 ;;
    --ssh)   SSH="$2";   shift 2 ;;
    -h|--help) usage ;;
    *) echo "❌ argumento desconhecido: $1"; usage ;;
  esac
done

# ── validação de entrada ────────────────────────────────────────────────
[[ -z "$ID" ]] && { echo "❌ --id é obrigatório (ex: vps)"; usage; }
[[ -z "$HOST" ]] && { echo "❌ --host é obrigatório (ex: 100.65.150.57)"; usage; }
[[ "$ID" =~ ^[a-z0-9]+$ ]] || { echo "❌ --id deve ser [a-z0-9]+ (ex: vps, lubuntu)"; exit 1; }
[[ "$PORT" =~ ^[0-9]+$ ]] && (( PORT >= 1 && PORT <= 65535 )) || { echo "❌ --port inválido: $PORT"; exit 1; }
[[ "$COLOR" =~ ^#[0-9a-fA-F]{6}$ ]] || { echo "❌ --color deve ser hex (#rrggbb): $COLOR"; exit 1; }

ID_UPPER="$(echo "$ID" | tr '[:lower:]' '[:upper:]')"
NAME="${NAME:-$ID}"
BASE="http://${HOST}:${PORT}"

# ── 1. valida conectividade ANTES de tocar no systemd ───────────────────
echo "🔍 Validando conectividade de ${ID} em ${BASE} ..."
HEALTH_URL="${BASE}/api/pc-health"
if ! curl -fsS --max-time "$HEALTH_TIMEOUT" "$HEALTH_URL" >/dev/null 2>&1; then
  # fallback: /api/session?limit=1 (alguns nodes não expõem pc-health)
  if ! curl -fsS --max-time "$HEALTH_TIMEOUT" "${BASE}/api/session?limit=1" >/dev/null 2>&1; then
    echo "❌ Node ${ID} inalcançável em ${BASE} (health check falhou)."
    echo "   Verifique host/porta e que 'opencode serve' está no ar no node."
    echo "   Nada foi alterado no systemd."
    exit 1
  fi
fi
echo "✅ Node ${ID} respondeu em ${BASE}."

# ── 2. backup do drop-in atual (rollback) ───────────────────────────────
BACKUP=""
if [[ -f "$DROPIN" ]]; then
  BACKUP="${DROPIN}.bak-$(date +%s)"
  cp "$DROPIN" "$BACKUP"
  echo "📦 Backup do drop-in atual: $BACKUP"
fi

# ── 3. escreve o drop-in ────────────────────────────────────────────────
mkdir -p "$DROPIN_DIR"
{
  echo "# gerado por add-node.sh em $(date -Is)"
  echo "[Service]"
  echo "Environment=NODES_${ID_UPPER}_HOST=${HOST}"
  echo "Environment=NODES_${ID_UPPER}_PORT=${PORT}"
  echo "Environment=NODES_${ID_UPPER}_NAME=${NAME}"
  echo "Environment=NODES_${ID_UPPER}_COLOR=${COLOR}"
  if [[ -n "$SSH" ]]; then
    echo "Environment=NODES_${ID_UPPER}_SSH=${SSH}"
  fi
} > "$DROPIN"
echo "✍️  Drop-in escrito: $DROPIN"

rollback() {
  echo "⚠️  Rollback: restaurando drop-in anterior..."
  if [[ -n "$BACKUP" ]]; then
    mv "$BACKUP" "$DROPIN"
  else
    rm -f "$DROPIN"
  fi
  systemctl --user daemon-reload
  systemctl --user restart "$UNIT" 2>/dev/null || true
  echo "↩️  Drop-in restaurado. Serviço reiniciado."
}

# ── 4. aplica (daemon-reload + restart) ─────────────────────────────────
echo "🔄 Aplicando: daemon-reload + restart do ${UNIT} ..."
if ! systemctl --user daemon-reload; then
  echo "❌ daemon-reload falhou."; rollback; exit 1
fi
if ! systemctl --user restart "$UNIT"; then
  echo "❌ restart do serviço falhou."; rollback; exit 1
fi

# ── 5. confere em /api/nodes ────────────────────────────────────────────
echo "⏳ Aguardando o proxy subir e agregar o node ${ID} ..."
sleep 3
NODES_JSON="$(curl -fsS --max-time 10 "${PROXY_URL}/api/nodes" 2>/dev/null || true)"
if [[ -z "$NODES_JSON" ]]; then
  echo "❌ Não consegui ler ${PROXY_URL}/api/nodes após o restart."
  echo "   O node pode ter sido adicionado mesmo assim — confira manualmente."
  exit 1
fi

if echo "$NODES_JSON" | grep -q "\"id\":\"${ID}\""; then
  if echo "$NODES_JSON" | grep -q "\"id\":\"${ID}\"" && echo "$NODES_JSON" | grep -q "\"reachable\":true"; then
    echo "✅ Node ${ID} adicionado e reachable:true em /api/nodes."
  else
    echo "⚠️  Node ${ID} presente em /api/nodes, mas reachable não é true."
    echo "   Confira a conectividade do node e o log do proxy."
  fi
else
  echo "❌ Node ${ID} NÃO apareceu em /api/nodes após o restart."
  rollback
  exit 1
fi

echo "🎉 Pronto. Node ${ID} (${NAME}) ativo no proxy."
