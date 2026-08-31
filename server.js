/**
 * opencode-pwa proxy server
 *
 * - Serve os estáticos do PWA (index.html, app.js, style.css, ...)
 * - Faz proxy para a API real do opencode (porta 4333) com streaming (SSE)
 * - Injeção de auth Basic via auth.local.json (GITIGNORED) ou env vars
 * - Logging estruturado JSON em logs/server.log (níveis: debug|info|warn|error)
 */
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');

const API_HOST = process.env.API_HOST || '127.0.0.1';
const API_PORT = parseInt(process.env.API_PORT || '4333', 10);
const PORT = parseInt(process.env.PORT || '4335', 10);

// HTTPS opcional: HTTPS_CERT + HTTPS_KEY apontam para os arquivos do cert.
// Sem eles, roda HTTP puro (comportamento atual). Para Tailscale, um
// self-signed basta; para VPS público, use Let's Encrypt.
const HTTPS_CERT = process.env.HTTPS_CERT || '';
const HTTPS_KEY = process.env.HTTPS_KEY || '';
const USE_HTTPS = !!(HTTPS_CERT && HTTPS_KEY);

// ============================================================
// NODES — multi-PC. Registro de upstreams de opencode serve.
// O proxy agrega sessões/health de todos os nodes e roteia
// prompts pelo node dono da sessão. Config via NODES_JSON (JSON
// string, ex: '[{"id":"main","name":"Main","color":"#3fb950",...}]')
// ou NODES_<ID>_HOST/PORT/NAME/COLOR por node.
// ============================================================
function loadNodes() {
  const raw = process.env.NODES_JSON;
  let nodes = [];
  if (raw) {
    try {
      nodes = JSON.parse(raw).map((n) => {
        // NODES_JSON pode omitir 'base'; deriva de host:port quando ausente.
        if (!n.base && n.host && n.port) return { ...n, base: `http://${n.host}:${n.port}` };
        return n;
      });
    } catch (e) { console.error('[FATAL] NODES_JSON inválido:', e.message); process.exit(1); }
  } else {
    const defaultNode = {
      id: 'main',
      name: process.env.NODE_MAIN_NAME || 'Main',
      color: process.env.NODE_MAIN_COLOR || '#3fb950',
      host: API_HOST,
      port: API_PORT,
      base: `http://${API_HOST}:${API_PORT}`
    };
    // nodes extras por env NODES_<ID>_HOST/PORT/NAME/COLOR
    const envKeys = Object.keys(process.env).filter((k) => /^NODES_[A-Z0-9]+_HOST$/.test(k));
    const extra = [];
    for (const k of envKeys) {
      const id = k.match(/^NODES_([A-Z0-9]+)_HOST$/)[1].toLowerCase();
      const port = parseInt(process.env[`NODES_${id.toUpperCase()}_PORT`] || '4333', 10);
      const host = process.env[k];
      extra.push({
        id,
        name: process.env[`NODES_${id.toUpperCase()}_NAME`] || id,
        color: process.env[`NODES_${id.toUpperCase()}_COLOR`] || '#58a6ff',
        host,
        port,
        base: `http://${host}:${port}`,
        ssh: process.env[`NODES_${id.toUpperCase()}_SSH`] || null,
      });
    }
    nodes = [defaultNode, ...extra];
  }
  return nodes;
}
const NODES = loadNodes();
const NODE_BY_ID = new Map(NODES.map((n) => [n.id, n]));
// alias do node "main" para o upstream configurado diretamente (API_HOST/API_PORT)
NODE_BY_ID.get('main').base = `http://${API_HOST}:${API_PORT}`;
NODE_BY_ID.get('main').host = API_HOST;
NODE_BY_ID.get('main').port = API_PORT;
const DEFAULT_NODE = NODES[0];

// ---------------------------------------------------------------------------
// Node metadata persistente (nomes/cores customizados pelo app).
// Salvo em node-names.json junto ao proxy; carregado na inicialização e
// aplicado sobre os defaults do loadNodes(). Assim renomear no app vale
// para TODOS os clientes (server-side), e sobrevive a restarts do proxy.
// ---------------------------------------------------------------------------
const NODE_NAMES_FILE = path.join(__dirname, 'node-names.json');
function loadNodeNames() {
  try { return JSON.parse(fs.readFileSync(NODE_NAMES_FILE, 'utf8')); }
  catch (_) { return {}; } // arquivo ausente/inválido -> sem overrides
}
const nodeNames = loadNodeNames();
for (const [id, n] of NODE_BY_ID) {
  const saved = nodeNames[id];
  if (saved) {
    if (saved.name) n.name = saved.name;
    if (saved.color) n.color = saved.color;
  }
}
function saveNodeNames() {
  try { fs.writeFileSync(NODE_NAMES_FILE, JSON.stringify(nodeNames, null, 2)); return true; }
  catch (e) { logger.error('node_names.save_failed', { error: e.message }); return false; }
}

function nodeById(id) {
  return NODE_BY_ID.get(id) || null;
}

// ---------------------------------------------------------------------------
// Fail-fast + negative cache de nodes offline.
// Evita que o timeout do fetch (6s) trave o proxy em cada poll do app.
// ---------------------------------------------------------------------------
const nodeDownCache = new Map(); // nodeId -> timestampDown
const NODE_DOWN_TTL_MS = parseInt(process.env.NODE_DOWN_TTL_MS || '15000', 10);

function isNodeDown(id) {
  const ts = nodeDownCache.get(id);
  if (!ts) return false;
  if ((Date.now() - ts) > NODE_DOWN_TTL_MS) {
    nodeDownCache.delete(id);
    return false;
  }
  return true;
}

function markNodeDown(id, reason) {
  if (!nodeDownCache.has(id)) {
    const n = nodeById(id);
    logger.warn('node.down', { node: id, name: n?.name, reason });
  }
  nodeDownCache.set(id, Date.now());
}

function markNodeUp(id) {
  if (nodeDownCache.has(id)) {
    const n = nodeById(id);
    logger.info('node.up', { node: id, name: n?.name });
    nodeDownCache.delete(id);
  }
}

// Encontra em qual node a sessão vive (checa a lista agregada em cache,
// senão consulta os nodes em paralelo). Cache TTL curto pra não estourar.
const sessionNodeCache = new Map();
const SESSION_NODE_TTL_MS = parseInt(process.env.SESSION_NODE_TTL_MS || '5000', 10);
let sessionNodeCacheTs = 0;

async function findSessionNode(sessionId) {
  const cachedId = sessionNodeCache.get(sessionId);
  if (cachedId && (Date.now() - sessionNodeCacheTs) < SESSION_NODE_TTL_MS) {
    return nodeById(cachedId) || null;
  }
  const jobs = NODES.map(async (n) => {
    if (isNodeDown(n.id)) return null; // fail-fast: node sabidamente offline
    try {
      const res = await fetch(`${n.base}/session/${encodeURIComponent(sessionId)}`, {
        headers: { Authorization: AUTH },
        signal: AbortSignal.timeout(1500),
      });
      if (!res.ok) return null;
      markNodeUp(n.id); // resposta saudável: cancela marcação de down
      return n;
    } catch (_) { return null; }
  });
  const found = (await Promise.all(jobs)).find(Boolean) || null;
  if (found) {
    sessionNodeCache.set(sessionId, found.id);
    sessionNodeCacheTs = Date.now();
  }
  return found;
}
const PHONE_AGENT = process.env.PHONE_AGENT || 'Sisyphus-Junior';
const PHONE_MODEL = process.env.PHONE_MODEL || '9router/ollama/gpt-oss:120b';
const PHONE_MODEL_BIG = process.env.PHONE_MODEL_BIG || '9router/combo-round-robin';
// default ABSOLUTO do celular (diretiva do usuário): nunca trocar por outro
const PHONE_DEFAULT_MODEL = process.env.PHONE_DEFAULT_MODEL || 'opencode/big-pickle';
const PHONE_BIG_SESSION_BYTES = parseInt(process.env.PHONE_BIG_SESSION_BYTES || '400000', 10);

// ============================================================
// VISÃO — descrição de imagens anexadas no chat.
// O proxy intercepta mensagens com imagem, chama um modelo de
// visão do 9router (OpenAI-compat) para descrever a imagem e
// injeta a descrição no prompt do modelo principal da sessão.
// Config via env: NINEROUTER_URL / NINEROUTER_KEY / VISION_MODELS
// ============================================================
const NINEROUTER_URL = (process.env.NINEROUTER_URL || 'http://localhost:20128').replace(/\/+$/, '');
const NINEROUTER_KEY = process.env.NINEROUTER_KEY || 'sk-47cf0a5d2c5c4000-zjk3df-94c6d9ba';
// cadeia de fallback: tenta cada modelo de visão em ordem até um responder
const VISION_MODELS = (process.env.VISION_MODELS || 'nvidia/minimaxai/minimax-m3,seek/gemini-3-6-flash,rw/qwen/qwen2.5-vl-72b-instruct,rw/qwen/qwen3-vl-8b-instruct')
  .split(',').map((s) => s.trim()).filter(Boolean);
// visão é best-effort: o client mobile aborta em ~15s, então a descrição NUNCA
// pode travar o envio do prompt. Deadline duro total + timeout curto por modelo.
// Providers de visão do 9router estão majoritariamente esgotados (429/402/500),
// então o deadline é curto para o fallback entrar rápido e o prompt não atrasar.
const VISION_DEADLINE_MS = Number(process.env.VISION_DEADLINE_MS || 4000);
const VISION_TIMEOUT_MS = Number(process.env.VISION_TIMEOUT_MS || 3000);

// ── GUARD de repetição degenerada ──────────────────────────────────────────
// Causa raiz dos loops de 2026-08-29: o modelo (opencode/big-pickle) degenerou
// e produziu UMA resposta com 2346 repetições de uma mesma frase (84KB). Este
// guard detecta esse padrão no stream de texto que passa pelo proxy e:
//   (1) trunca a live part para o app não receber o lixo infinito,
//   (2) dispara abort do run no serve (fire-and-forget).
// Heurística: dividir o texto em "sentenças" (fragmentos >= DEGENERATION_MIN_SENTENCE_LEN)
// e, se UMA sentença se repetir >= DEGENERATION_REPEAT_THRESHOLD vezes, é degeneração.
const DEGENERATION_REPEAT_THRESHOLD = Number(process.env.DEGENERATION_REPEAT_THRESHOLD || 20);
const DEGENERATION_MIN_SENTENCE_LEN = Number(process.env.DEGENERATION_MIN_SENTENCE_LEN || 15);
const DEGENERATION_CHECK_INTERVAL = Number(process.env.DEGENERATION_CHECK_INTERVAL || 2000); // chars crescidos entre checks
const DEGENERATION_MAX_CHECK_TEXT = Number(process.env.DEGENERATION_MAX_CHECK_TEXT || 40000); // janela max analisada/check
const DEGENERATION_KEEP = Number(process.env.DEGENERATION_KEEP || 2000); // chars mantidos na part truncada
const DEGENERATION_NOTICE = '\n\n[⚠️ resposta interrompida pelo proxy — repetição degenerada detectada]';

// Detecta degeneração por repetição de sentença num texto acumulado.
// Segura para streaming: só roda a cada DEGENERATION_CHECK_INTERVAL chars e numa
// janela limitada (DEGENERATION_MAX_CHECK_TEXT), então custo é O(1) amortizado.
function detectDegeneration(text) {
  if (!text || text.length < DEGENERATION_MIN_SENTENCE_LEN) return null;
  const window = text.slice(-DEGENERATION_MAX_CHECK_TEXT);
  const counts = new Map();
  // quebra em sentenças por pontuação forte / quebra de linha / ponto-e-vírgula
  const sentences = window.split(/[.!?\n;]+/);
  for (let raw of sentences) {
    const s = raw.trim();
    if (s.length < DEGENERATION_MIN_SENTENCE_LEN) continue;
    const n = (counts.get(s) || 0) + 1;
    counts.set(s, n);
    if (n >= DEGENERATION_REPEAT_THRESHOLD) return { sentence: s.slice(0, 80), count: n };
  }
  return null;
}

// Descreve uma imagem (base64) usando o primeiro modelo de visão que responder.
// Retorna a descrição em texto, ou null se todos falharem ou estourar o deadline.
async function describeImage(dataUrl) {
  if (!dataUrl || typeof dataUrl !== 'string') return null;
  const deadline = Date.now() + VISION_DEADLINE_MS;
  for (const model of VISION_MODELS) {
    if (Date.now() > deadline) break;
    try {
      const ctrl = new AbortController();
      const remaining = Math.max(1000, deadline - Date.now());
      const timer = setTimeout(() => ctrl.abort(), Math.min(VISION_TIMEOUT_MS, remaining));
      const res = await fetch(`${NINEROUTER_URL}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(NINEROUTER_KEY ? { Authorization: `Bearer ${NINEROUTER_KEY}` } : {}),
        },
        body: JSON.stringify({
          model,
          messages: [{
            role: 'user',
            content: [
              { type: 'text', text: 'Descreva esta imagem em detalhes, em português. Liste os elementos visuais principais, cores, texto legível, e qualquer informação relevante. Seja objetivo e completo.' },
              { type: 'image_url', image_url: { url: dataUrl } },
            ],
          }],
          max_tokens: 500,
        }),
        signal: ctrl.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        logger.warn('vision.model_failed', { model, status: res.status, resp: body.slice(0, 200) });
        continue;
      }
      const raw = await res.text();
      // o 9router pode anexar "data: [DONE]" (SSE) após o JSON — remove antes de parsear
      const jsonStr = raw.split('data: [DONE]')[0].trim();
      const j = JSON.parse(jsonStr);
      const desc = j?.choices?.[0]?.message?.content;
      if (desc && typeof desc === 'string' && desc.trim()) {
        logger.info('vision.described', { model, chars: desc.length });
        return desc.trim();
      }
      logger.warn('vision.empty', { model });
    } catch (e) {
      logger.warn('vision.error', { model, error: e.message });
    }
  }
  logger.error('vision.all_failed', { models: VISION_MODELS });
  return null;
}

function parseModelRef(ref) {
  const [providerID, ...rest] = ref.split('/');
  return rest.length ? { providerID, modelID: rest.join('/') } : null;
}

function sessionContextBytes(sessionId) {
  const d = openDb();
  if (!d) return 0;
  try {
    const m = d.prepare('SELECT COALESCE(SUM(LENGTH(data)),0) AS s FROM message WHERE session_id = ?').get(sessionId);
    const p = d.prepare('SELECT COALESCE(SUM(LENGTH(data)),0) AS s FROM part WHERE session_id = ?').get(sessionId);
    return ((m && m.s) || 0) + ((p && p.s) || 0);
  } catch (_) { return 0; }
}

// ============================================================
// CREDENCIAIS — NUNCA hardcoded. Prioridade:
//   1. OPENCODE_USERNAME / OPENCODE_PASSWORD (env)
//   2. auth.local.json (gitignored, mesmo diretório)
//   3. FALHA (não sobe sem credenciais)
// ============================================================
function loadAuth() {
  const envU = process.env.OPENCODE_USERNAME;
  const envP = process.env.OPENCODE_PASSWORD;
  if (envU && envP) return { username: envU, password: envP };

  const authFile = path.join(__dirname, 'auth.local.json');
  try {
    const data = JSON.parse(fs.readFileSync(authFile, 'utf8'));
    if (data.username && data.password) return data;
  } catch (e) { /* arquivo ausente ou inválido */ }

  console.error('[FATAL] Credenciais não encontradas. Defina OPENCODE_USERNAME/OPENCODE_PASSWORD ou crie auth.local.json');
  process.exit(1);
}
let AUTH = (() => {
  const { username, password } = loadAuth();
  return 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
})();

// Rotação de credenciais sem restart: recarrega auth.local.json (SIGHUP).
// Env vars continuam exigindo restart (systemd), mas o arquivo permite troca
// a quente. Se o arquivo sumir/invalidar, mantém a credencial atual.
function reloadAuth() {
  const authFile = path.join(__dirname, 'auth.local.json');
  try {
    const data = JSON.parse(fs.readFileSync(authFile, 'utf8'));
    if (data.username && data.password) {
      AUTH = 'Basic ' + Buffer.from(`${data.username}:${data.password}`).toString('base64');
      logger.info('auth.reloaded', { source: 'auth.local.json' });
    }
  } catch (e) {
    logger.warn('auth.reload_failed', { message: e.message });
  }
}
process.on('SIGHUP', reloadAuth);

// ============================================================
// LOGGER — JSON estruturado em logs/server.log
// Formato por linha: {"ts":ISO,"level":"info","event":"...","data":{...}}
// Níveis: debug < info < warn < error. Controle: LOG_LEVEL (default: info)
// ============================================================
const LOG_DIR = path.join(__dirname, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'server.log');
const LOG_LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(level, event, data) {
  const lvl = LOG_LEVELS[level] !== undefined ? level : 'info';
  if (LOG_LEVELS[lvl] < (LOG_LEVELS[LOG_LEVEL] || LOG_LEVELS.info)) return;
  const entry = {
    ts: new Date().toISOString(),
    level: lvl,
    event,
    data: data || {}
  };
  const line = JSON.stringify(entry) + '\n';
  try { fs.appendFileSync(LOG_FILE, line); } catch (_) { /* best-effort */ }
  if (process.env.PROXY_DEBUG || lvl === 'error' || lvl === 'warn') console.log(line.trim());
}
const logger = {
  debug: (ev, d) => log('debug', ev, d),
  info:  (ev, d) => log('info',  ev, d),
  warn:  (ev, d) => log('warn',  ev, d),
  error: (ev, d) => log('error', ev, d)
};

// ============================================================
// NTFY PUSH — notificação de falhas do forward (o celular descobre
// sem precisar abrir o app). Config em ~/.config/opencode/notify.local.json
// (mesma usada pelo plugin agent-status.js). Fire-and-forget.
// ============================================================
const NOTIFY_CFG = path.join(os.homedir(), '.config/opencode/notify.local.json');
let NOTIFY = null;
try {
  const nd = JSON.parse(fs.readFileSync(NOTIFY_CFG, 'utf8'));
  if (nd.server && nd.topic) NOTIFY = nd;
} catch (_) { /* sem config — push desativado */ }

function ntfyPush(title, message, tags) {
  if (!NOTIFY) return;
  try {
    const payload = JSON.stringify({ topic: NOTIFY.topic, title, message, tags: tags || ['iphone.radiowaves.left.and.right'] });
    const u = new URL(NOTIFY.server.endsWith('/') ? NOTIFY.server : NOTIFY.server + '/');
    const nreq = http.request(u, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      timeout: 5000
    }, (nres) => nres.resume());
    nreq.on('error', () => {});
    nreq.on('timeout', () => nreq.destroy());
    nreq.end(payload);
  } catch (_) { /* best-effort */ }
}

// ============================================================
// SQLITE BRIDGE — sessões criadas na TUI (PC) não são
// materializadas pelo processo serve em `session_message`,
// então `/api/session/{id}/message` retorna vazio e o SSE não
// emite eventos. A bridge lê `message` + `part` direto do banco
// (WAL, read-only, seguro) e sintetiza as respostas no MESMO
// formato da API. Zero dependências: usa node:sqlite nativo.
// ============================================================
const DB_PATH = process.env.OPENCODE_DB || path.join(os.homedir(), '.local/share/opencode/opencode.db');
let sqlite = null;
let db = null;

function openDb() {
  if (db) return db;
  try {
    sqlite = require('node:sqlite');
    db = new sqlite.DatabaseSync(DB_PATH, { readOnly: true });
    db.exec('PRAGMA busy_timeout = 3000');
    logger.info('bridge.db_open', { path: DB_PATH });
  } catch (e) {
    logger.warn('bridge.db_failed', { error: e.message });
    db = null;
  }
  return db;
}

// Quantas mensagens a sessão tem no SQLite (para decidir se sintetizamos)
function bridgeHasMessages(sessionId) {  const d = openDb();
  if (!d) return false;
  try {
    const r = d.prepare('SELECT COUNT(*) AS c FROM message WHERE session_id = ?').get(sessionId);
    return (r && r.c > 0) || false;
  } catch (e) {
    logger.warn('bridge.count_error', { sessionId, error: e.message });
    return false;
  }
}

// Monta uma mensagem no shape exato da API a partir de message + part
function buildBridgeMessage(mrow) {
  let mdata = {};
  try { mdata = JSON.parse(mrow.data || '{}'); } catch (_) {}
  const role = mdata.role || 'user';

  // parts desta mensagem
  let parts = [];
  try {
    parts = db.prepare(
      'SELECT id, data, time_created, time_updated FROM part WHERE message_id = ? ORDER BY rowid ASC'
    ).all(mrow.id).map(p => {
      let pd = {};
      try { pd = JSON.parse(p.data || '{}'); } catch (_) {}
      return { id: p.id, time_created: p.time_created, time_updated: p.time_updated, data: pd };
    });
  } catch (e) {
    logger.warn('bridge.parts_error', { messageId: mrow.id, error: e.message });
  }

  const base = {
    id: mrow.id,
    time: { created: mdata.time?.created || mrow.time_created }
  };

  if (role === 'user') {
    // API devolve {id, text, time, type:'user'}
    const textPart = parts.find(p => p.data.type === 'text');
    return {
      ...base,
      type: 'user',
      text: textPart ? (textPart.data.text || '') : ''
    };
  }

  // assistant: content[] com reasoning/text/tool na ordem das parts
  const content = [];
  let finish = null;
  let cost = 0;
  let tokens = null;
  let hasStepFinish = false;
  let lastUpdated = mrow.time_updated || mrow.time_created;

  // erro do modelo (ex.: rate limit 429) gravado pelo serve em message.error —
  // sem isto o app vê spinner eterno e depois "(sem resposta)" sem explicação
  let error = null;
  if (mdata.error) {
    const ed = mdata.error.data || {};
    error = {
      name: mdata.error.name || 'Error',
      message: ed.message || 'erro desconhecido',
      statusCode: ed.statusCode || null
    };
  }

  for (const p of parts) {
    const d = p.data;
    if (d.type === 'reasoning' || d.type === 'text') {
      content.push({ type: d.type, text: d.text || '' });
      if (p.time_updated > lastUpdated) lastUpdated = p.time_updated;
    } else if (d.type === 'tool') {
      content.push({
        type: 'tool',
        name: d.tool || 'tool',
        callID: d.callID || null,
        state: d.state || {}
      });
      if (p.time_updated > lastUpdated) lastUpdated = p.time_updated;
    } else if (d.type === 'step-finish') {
      hasStepFinish = true;
      if (d.reason) finish = d.reason;
      if (d.cost !== undefined) cost = d.cost;
      if (d.tokens) tokens = d.tokens;
      if (p.time_updated > lastUpdated) lastUpdated = p.time_updated;
    }
  }

  // model no shape da API {id, providerID, variant}
  let model = null;
  if (mdata.modelID || mdata.model) {
    const m = mdata.model || {};
    model = {
      id: mdata.modelID || m.modelID || m.id || 'unknown',
      providerID: m.providerID || 'opencode',
      variant: m.variant || 'default'
    };
  }

  return {
    ...base,
    time: { ...base.time, completed: lastUpdated },
    type: 'assistant',
    inProgress: !hasStepFinish && !error,
    agent: mdata.agent || null,
    model,
    finish: finish || mdata.finish || 'stop',
    cost: mdata.cost !== undefined ? mdata.cost : cost,
    tokens: mdata.tokens || tokens,
    content,
    error
  };
}

// Sintetiza /api/session/{id}/message (mesmo shape: {data, cursor}, DESC)
function bridgeMessages(sessionId, limit) {
  const d = openDb();
  if (!d) return null;
  const lim = Math.min(Math.max(limit || 100, 1), 500);
  try {
    const rows = d.prepare(
      'SELECT id, data, time_created, time_updated FROM message WHERE session_id = ? ORDER BY rowid DESC LIMIT ?'
    ).all(sessionId, lim);
    if (!rows.length) return null;
    const data = rows.map(buildBridgeMessage);
    return { data, cursor: { previous: null, next: null } };
  } catch (e) {
    logger.error('bridge.messages_error', { sessionId, error: e.message });
    return null;
  }
}

// Reordena /api/session por time.updated DESC (as sessões ativas sobem)
function bridgeSortSessions(list) {
  if (!Array.isArray(list)) return list;
  return list.slice().sort((a, b) => (b.time?.updated || 0) - (a.time?.updated || 0));
}

// ── pc-health ────────────────────────────────────────────────
let healthCache = { ts: 0, data: null };
let cpuPrevSample = null;

function readCpuSample() {
  const parts = fs.readFileSync('/proc/stat', 'utf8').split('\n')[0].trim().split(/\s+/).slice(1).map(Number);
  const idle = (parts[3] || 0) + (parts[4] || 0);
  return { idle, total: parts.reduce((a, b) => a + b, 0) };
}

async function collectHealth() {
  if (!cpuPrevSample) cpuPrevSample = readCpuSample();
  await new Promise((r) => setTimeout(r, 150));
  const cur = readCpuSample();
  let cpu = null;
  const dT = cur.total - cpuPrevSample.total;
  const dI = cur.idle - cpuPrevSample.idle;
  if (dT > 0) cpu = Math.max(0, Math.min(100, Math.round((1 - dI / dT) * 100)));
  cpuPrevSample = cur;

  let temp = null;
  try {
    for (const z of fs.readdirSync('/sys/class/thermal')) {
      if (!z.startsWith('thermal_zone')) continue;
      const type = fs.readFileSync(`/sys/class/thermal/${z}/type`, 'utf8').trim();
      if (type === 'x86_pkg_temp') {
        temp = Math.round(parseInt(fs.readFileSync(`/sys/class/thermal/${z}/temp`, 'utf8').trim(), 10) / 1000);
        break;
      }
    }
  } catch (_) {}

  let ram = null;
  try {
    const mi = {};
    for (const l of fs.readFileSync('/proc/meminfo', 'utf8').split('\n')) {
      const m = l.match(/^(\w+):\s+(\d+)/);
      if (m) mi[m[1]] = parseInt(m[2], 10);
    }
    if (mi.MemTotal && mi.MemAvailable !== undefined) {
      const usedKb = mi.MemTotal - mi.MemAvailable;
      ram = {
        usedGb: +(usedKb / 1048576).toFixed(1),
        totalGb: +(mi.MemTotal / 1048576).toFixed(1),
        pct: Math.round((usedKb / mi.MemTotal) * 100),
      };
    }
  } catch (_) {}

  let battery = null;
  try {
    const base = '/sys/class/power_supply';
    const bats = fs.readdirSync(base).filter((d) => /^BAT/.test(d));
    if (bats.length) {
      const b = bats[0];
      const pct = parseInt(fs.readFileSync(`${base}/${b}/capacity`, 'utf8').trim(), 10);
      const status = fs.readFileSync(`${base}/${b}/status`, 'utf8').trim().toLowerCase();
      let plugged = false;
      try {
        plugged = fs.readFileSync(`${base}/ADP0/online`, 'utf8').trim() === '1';
      } catch (_) {}
      battery = { pct, charging: status === 'charging' || (plugged && status !== 'discharging' && status !== 'not charging'), status };
    }
  } catch (_) {}

  return { ts: Date.now(), cpu, temp, ram, battery, uptimeSec: Math.round(os.uptime()) };
}

// ============================================================
// NODE HEALTH — telemetria por node (PC remoto).
// - Node local ("main"): coleta via /proc (collectHealth)
// - Node remoto: 1º tenta HTTP no opencode serve do node (se
//   expõe /api/pc-health); 2º fallback SSH (coleta via ssh remoto).
// Cache por node (TTL 2s) pra suportar o poll do app.
// ============================================================
const nodeHealthCache = new Map(); // nodeId -> { ts, data }
const NODE_HEALTH_TTL_MS = parseInt(process.env.NODE_HEALTH_TTL_MS || '2000', 10);

async function nodeHealth(node, force) {
  if (isNodeDown(node.id)) throw new Error('health indisponível'); // fail-fast
  const cached = nodeHealthCache.get(node.id);
  if (!force && cached && (Date.now() - cached.ts) < NODE_HEALTH_TTL_MS) {
    return cached.data;
  }
  let data = null;
  if (node.id === 'main') {
    data = await collectHealth();
  } else {
    try {
      const res = await fetch(`${node.base}/api/pc-health`, { signal: AbortSignal.timeout(2500) });
      if (res.ok) { data = await res.json(); }
    } catch (_) {}
    if (!data && process.env.SSH_BIN && node.ssh) {
      try {
        const { execFile } = require('child_process');
        const { promisify } = require('util');
        const pexec = promisify(execFile);
        const ident = process.env.SSH_IDENTITY || `${os.homedir()}/.ssh/id_ed25519_ospos`;
        const out = await pexec('/bin/sh', ['-c', `${process.env.SSH_BIN} -i ${ident} -o BatchMode=yes -o ConnectTimeout=4 ${node.ssh} 'cat /proc/uptime; echo; free -m | sed -n 2p; cat /proc/stat | sed -n 1p; cat /sys/class/thermal/thermal_zone2/type 2>/dev/null; cat /sys/class/thermal/thermal_zone2/temp 2>/dev/null'`], { timeout: 6000 });
        data = parseSshHealth(out.stdout, node.id);
        logger.debug('node.health_ssh', { node: node.id });
      } catch (e) {
        logger.debug('node.health_ssh_failed', { node: node.id, error: e.message });
      }
    }
  }
  if (!data) {
    markNodeDown(node.id, 'health indisponível');
    throw new Error('health indisponível');
  }
  markNodeUp(node.id);
  nodeHealthCache.set(node.id, { ts: Date.now(), data });
  logger.debug('node.health_cached', { node: node.id });
  return data;
}

const nodeCpuSamples = new Map(); // nodeId -> { idle, total, ts }

function parseSshHealth(raw, nodeId) {
  const lines = raw.split('\n').filter((l) => l.trim());
  let cpu = null, temp = null, ram = null, battery = null, uptimeSec = null;
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i];
    const parts = l.trim().split(/\s+/);
    if (parts.length === 2 && !isNaN(parseFloat(parts[0])) && !isNaN(parseFloat(parts[1]))) {
      uptimeSec = Math.round(parseFloat(parts[0]));
    } else if (/^Mem/i.test(l) && parts.length >= 3) {
      const totalMb = parseInt(parts[1], 10);
      const usedMb = parseInt(parts[2], 10);
      if (totalMb) ram = { usedGb: +(usedMb / 1024).toFixed(1), totalGb: +(totalMb / 1024).toFixed(1), pct: Math.round((usedMb / totalMb) * 100) };
    } else if (/^cpu\s/i.test(l) && parts.length >= 8) {
      const nums = parts.slice(1, 8).map(Number);
      const idle = (nums[3] || 0) + (nums[4] || 0);
      const total = nums.reduce((a, b) => a + b, 0);
      const prev = nodeCpuSamples.get(nodeId);
      if (prev) {
        const dT = total - prev.total;
        const dI = idle - prev.idle;
        if (dT > 0) cpu = Math.max(0, Math.min(100, Math.round((1 - dI / dT) * 100)));
      }
      nodeCpuSamples.set(nodeId, { idle, total });
    } else if (/^x86_pkg_temp/i.test(l) && lines[i + 1] && !isNaN(parseInt(lines[i + 1].trim(), 10))) {
      temp = Math.round(parseInt(lines[i + 1].trim(), 10) / 1000);
      i++;
    }
  }
  return { ts: Date.now(), cpu, temp, ram, battery, uptimeSec };
}

// Upstream ordena por created; sessões TUI ativas ficam fora do top-50.
// Busca limit maior, reordena por updated desc, devolve top-N do cliente.
// Cache TTL curto colapsa a rajada de polling do app (2-3s por ciclo).
const SESSIONS_CACHE_TTL_MS = parseInt(process.env.SESSIONS_CACHE_TTL_MS || '3000', 10);
const sessionsCache = new Map();

// Cache guarda a lista COMPLETA ordenada; o corte por limit é feito na hora,
// senão um limit=1 (health check) envenena o cache do poll real de 50.
function proxySessionsSorted(req, res) {
  const reqLimit = parseInt(new URL(req.url, 'http://x').searchParams.get('limit') || '50', 10);
  const upstreamLimit = Math.max(reqLimit, 200);

  const cacheKey = `sess:${reqLimit}`;
  const cached = sessionsCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts) < SESSIONS_CACHE_TTL_MS && Array.isArray(cached.sortedFull)) {
    logger.debug('bridge.sessions_cache_hit', { requested: reqLimit, ageMs: Date.now() - cached.ts });
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify(cached.sortedFull.slice(0, reqLimit)));
  }

  // agrega sessões de TODOS os nodes em paralelo, taggeando cada uma com
  // {node: {id, name, color}} pra o app colorir o chat por PC.
  const jobs = NODES.map(async (n) => {
    if (isNodeDown(n.id)) return null; // fail-fast: pula node sabidamente offline
    try {
      const res = await fetch(`${n.base}/api/session?limit=${upstreamLimit}`, {
        headers: { Authorization: AUTH, Accept: 'application/json' },
        signal: AbortSignal.timeout(2500),
      });
      if (!res.ok) {
        markNodeDown(n.id, `http ${res.status}`);
        return null;
      }
      markNodeUp(n.id);
      const body = await res.json();
      const list = Array.isArray(body) ? body : (body.data || []);
      return list.map((s) => ({ ...s, node: { id: n.id, name: n.name, color: n.color } }));
    } catch (e) {
      markNodeDown(n.id, e.message);
      return null;
    }
  });
  Promise.all(jobs).then((results) => {
    const flat = results.filter(Boolean).flat();
    const sorted = bridgeSortSessions(flat);
    sessionsCache.set(cacheKey, { ts: Date.now(), sortedFull: sorted });
    logger.info('bridge.sessions_aggregated', {
      requested: reqLimit,
      nodes: NODES.length,
      total: flat.length,
      perNode: NODES.map((n) => [n.id, results[NODES.indexOf(n)]?.length || 0]),
    });
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(sorted.slice(0, reqLimit)));
  }).catch((e) => {
    logger.error('bridge.sessions_aggregate_error', { error: e.message });
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'bridge: ' + e.message }));
  });
}

// ============================================================
// BRIDGE SSE — sessões TUI não têm SSE upstream. Sintetiza:
// polling da tabela part (novos/atualizados) + novos user messages
// -> eventos no MESMO formato que o serve emite.
// ============================================================
function bridgeSSE(req, res, sessionId) {
  const d = openDb();
  if (!d) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'bridge: sqlite indisponível' }));
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  let seq = 0;
  try {
    const r = d.prepare('SELECT MAX(seq) AS s FROM event WHERE aggregate_id = ?').get(sessionId);
    seq = (r && r.s) || 0;
  } catch (_) {}

  // watermark: só eventos NOVOS (rowid > último visto na conexão)
  let lastMsgRowid = 0;
  let lastPartRowid = 0;
  try {
    const rm = d.prepare('SELECT MAX(rowid) AS r FROM message WHERE session_id = ?').get(sessionId);
    lastMsgRowid = (rm && rm.r) || 0;
    const rp = d.prepare('SELECT MAX(rowid) AS r FROM part WHERE session_id = ?').get(sessionId);
    lastPartRowid = (rp && rp.r) || 0;
  } catch (_) {}

  // estado por part: para emitir started/ended sem repetir
  const partState = new Map(); // partId -> {started, ended}
  const msgState = new Map();  // messageId -> já emitiu prompted

  const send = (type, data) => {
    seq += 1;
    const payload = {
      id: 'evt_bridge_' + Date.now().toString(36) + '_' + seq,
      type,
      durable: { aggregateID: sessionId, seq, version: 1 },
      data
    };
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  };

  const now = () => Date.now();

  const poll = () => {
    // 1. Novas mensagens do usuário -> session.next.prompted
    let newMsgs = [];
    try {
      newMsgs = d.prepare(
        "SELECT id, rowid, data FROM message WHERE session_id = ? AND rowid > ? AND json_extract(data, '$.role') = 'user' ORDER BY rowid ASC"
      ).all(sessionId, lastMsgRowid);
    } catch (_) {}
    for (const m of newMsgs) {
      if (m.rowid > lastMsgRowid) lastMsgRowid = m.rowid;
      if (msgState.has(m.id)) continue;
      msgState.set(m.id, true);
      let text = '';
      try {
        const tp = d.prepare(
          "SELECT data FROM part WHERE message_id = ? AND json_extract(data, '$.type') = 'text' ORDER BY rowid ASC LIMIT 1"
        ).get(m.id);
        if (tp) { try { text = JSON.parse(tp.data).text || ''; } catch (_) {} }
      } catch (_) {}
      send('session.next.prompted', {
        timestamp: now(), sessionID: sessionId, messageID: m.id,
        prompt: { text }, delivery: 'bridge'
      });
    }

    // 2. Parts novas/atualizadas -> eventos de step/reasoning/text/tool
    let newParts = [];
    try {
      newParts = d.prepare(
        'SELECT id, rowid, message_id, data, time_created, time_updated FROM part WHERE session_id = ? AND rowid > ? ORDER BY rowid ASC'
      ).all(sessionId, lastPartRowid);
    } catch (_) {}

    for (const p of newParts) {
      if (p.rowid > lastPartRowid) lastPartRowid = p.rowid;
      let pd = {};
      try { pd = JSON.parse(p.data || '{}'); } catch (_) {}
      const st = partState.get(p.id) || { started: false, ended: false };

      if (pd.type === 'step-start' && !st.started) {
        st.started = true;
        // agent/model vêm da message dona do step
        let agent = null, model = null;
        try {
          const mrow = d.prepare('SELECT data FROM message WHERE id = ?').get(p.message_id);
          if (mrow) {
            const md = JSON.parse(mrow.data || '{}');
            agent = md.agent || null;
            const m = md.model || {};
            model = {
              id: md.modelID || m.modelID || m.id || 'unknown',
              providerID: m.providerID || 'opencode',
              variant: m.variant || 'default'
            };
          }
        } catch (_) {}
        send('session.next.step.started', {
          timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
          agent, model
        });
      }
      else if (pd.type === 'reasoning') {
        if (!st.started) {
          st.started = true;
          send('session.next.reasoning.started', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            reasoningID: p.id
          });
        }
        if (!st.ended && pd.text) {
          st.ended = true;
          send('session.next.reasoning.ended', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            reasoningID: p.id, text: pd.text
          });
        }
      }
      else if (pd.type === 'text') {
        if (!st.started) {
          st.started = true;
          send('session.next.text.started', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            textID: p.id
          });
        }
        if (!st.ended && pd.text) {
          st.ended = true;
          send('session.next.text.ended', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            textID: p.id, text: pd.text
          });
        }
      }
      else if (pd.type === 'tool') {
        if (!st.started) {
          st.started = true;
          send('session.next.tool.input.started', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            name: pd.tool || 'tool', tool: pd.tool || 'tool', callID: pd.callID || null
          });
          if (pd.state && pd.state.input) {
            send('session.next.tool.called', {
              timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
              tool: pd.tool || 'tool', input: pd.state.input
            });
          }
        }
        if (!st.ended && pd.state) {
          const stt = pd.state.status;
          if (stt === 'completed') {
            st.ended = true;
            send('session.next.tool.success', {
              timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
              tool: pd.tool || 'tool', callID: pd.callID || null
            });
          } else if (stt === 'error') {
            st.ended = true;
            send('session.next.tool.failed', {
              timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
              tool: pd.tool || 'tool', error: pd.state.error || { message: 'tool failed' }
            });
          }
        }
      }
      else if (pd.type === 'step-finish') {
        if (!st.ended) {
          st.ended = true;
          send('session.next.step.ended', {
            timestamp: now(), sessionID: sessionId, assistantMessageID: p.message_id,
            tokens: pd.tokens || null, cost: pd.cost ?? 0, finish: pd.reason || 'stop'
          });
        }
      }

      partState.set(p.id, st);
    }

    // keep-alive
    res.write(': ping\n\n');
  };

  poll();
  const timer = setInterval(poll, 500);
  req.on('close', () => {
    clearInterval(timer);
    res.end();
  });
}

// ============================================================
// MIME types
// ============================================================
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.webmanifest': 'application/manifest+json'
};

// ============================================================
// PROXY — streaming para SSE, com logging de corpo/status/duração
// ============================================================
// ── live stream buffer: snapshot autoritativo do texto gerando AGORA ──
// O serve não persiste deltas; quem assiste /event vê uma vez só. O proxy
// acumula tudo aqui, então o app pode remontar o chat e buscar o texto
// completo sem depender de ter visto os eventos.
const liveParts = new Map();
// modelo atual por sessão via eventos session.updated (sync PC -> celular)
const sessionModels = new Map();
// último modelo que completou forward com sucesso por sessão (fallback 1º)
const sessionGoodModel = new Map();
// INVARIANTE: só UMA conexão /event acumula deltas; reconexões extras são
// espectadoras — senão o mesmo delta soma N vezes (bug do texto 4x duplicado)
let primaryTap = null;
// sessões cujo run já foi abortado por degeneração (evita spammar o abort)
const degenerateAborted = new Map(); // sessionId -> ts

// Aborta (fire-and-forget) o run de uma sessão degenerada no serve dono.
// Usa POST {base}/session/{id}/abort (único endpoint real do serve; o caminho
// /api/.../abort é HTML catch-all). Falha não derruba o proxy.
function abortDegenerateSession(sessionId, node) {
  const nid = node ? node.id : 'main';
  const last = degenerateAborted.get(sessionId);
  if (last && Date.now() - last < 30000) return; // no máx 1 abort/30s por sessão
  degenerateAborted.set(sessionId, Date.now());
  const base = node && node.base ? node.base : `http://${API_HOST}:${API_PORT}`;
  fetch(`${base}/session/${encodeURIComponent(sessionId)}/abort`, {
    method: 'POST',
    headers: { Authorization: AUTH, 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(5000),
  }).then((r) => {
    logger.warn('degeneration.aborted', { node: nid, sessionIdFull: sessionId, status: r.status, ok: r.ok });
  }).catch((e) => {
    logger.warn('degeneration.abort_failed', { node: nid, sessionIdFull: sessionId, error: e.message });
  });
}

function tapSseChunk(chunk, state) {
  if (!primaryTap || primaryTap.dead) primaryTap = state;
  const authoritative = state === primaryTap;
  const s = state.rem + chunk.toString('utf8');
  const lines = s.split('\n');
  state.rem = lines.pop();
  for (const line of lines) {
    if (!line.startsWith('data: ')) continue;
    let evt;
    try { evt = JSON.parse(line.slice(6)); } catch { continue; }
    const p = evt.properties || {};
    if (evt.type === 'session.updated' && p.info && p.info.id && p.info.model &&
        p.info.model.providerID && (p.info.model.id || p.info.model.modelID)) {
      sessionModels.set(p.info.id, { providerID: p.info.model.providerID, modelID: p.info.model.id || p.info.model.modelID });
      continue;
    }
    if (!authoritative) continue;
    if (evt.type === 'message.part.delta' && p.messageID && (p.field === 'text' || p.field === 'reasoning')) {
      const cur = liveParts.get(p.messageID) || { sessionId: p.sessionID, text: '', reasoning: '', ts: 0, checkAt: 0, aborted: false };
      if (p.field === 'text') {
        cur.text += p.delta || '';
        // GUARD degradação: a cada DEGENERATION_CHECK_INTERVAL chars crescidos,
        // varre a janela por repetição de sentença; se degenerar, trunca a part
        // (app não recebe o lixo) e aborta o run no serve.
        if (!cur.aborted && cur.text.length - (cur.checkAt || 0) >= DEGENERATION_CHECK_INTERVAL) {
          cur.checkAt = cur.text.length;
          const hit = detectDegeneration(cur.text);
          if (hit) {
            cur.aborted = true;
            cur.text = cur.text.slice(0, DEGENERATION_KEEP) + DEGENERATION_NOTICE;
            logger.error('degeneration.detected', {
              sessionId: (p.sessionID || '').slice(0, 14),
              messageID: p.messageID.slice(0, 14),
              sentence: hit.sentence,
              count: hit.count,
              textChars: cur.text.length,
            });
            const node = state.node || null;
            if (p.sessionID) abortDegenerateSession(p.sessionID, node);
          }
        }
      } else cur.reasoning += p.delta || '';
      cur.ts = Date.now();
      if (cur.text.length > 200000) cur.text = cur.text.slice(-150000);
      liveParts.set(p.messageID, cur);
    } else if (evt.type === 'message.part.updated' && p.messageID && p.part && typeof p.part.text === 'string') {
      const cur = liveParts.get(p.messageID);
      if (cur && !cur.aborted && p.part.text.length >= cur.text.length) {
        cur.text = p.part.text;
        cur.ts = Date.now();
      }
    } else if (evt.type === 'session.idle' && p.sessionID) {
      for (const [k, v] of liveParts) if (v.sessionId === p.sessionID) liveParts.delete(k);
    }
  }
}

// ── validação de modelo contra o catálogo do serve (cache 60s) ──
let modelCatalogCache = new Map(); // nodeId -> { ts, keys }

async function modelExists(model, node) {
  const nid = node ? node.id : 'main';
  const base = node && node.base ? node.base : `http://${API_HOST}:${API_PORT}`;
  try {
    const entry = modelCatalogCache.get(nid);
    if (!entry || Date.now() - entry.ts > 60000) {
      const res = await fetch(`${base}/config/providers`, {
        headers: { Authorization: AUTH },
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return true; // catálogo indisponível: não bloqueia (fail-open)
      const provs = await res.json();
      const list = Array.isArray(provs) ? provs : provs.providers || [];
      const keys = new Set();
      for (const p of list) {
        for (const k of Object.keys(p.models || {})) {
          keys.add(`${p.id}/${k}`);
          keys.add(k);
        }
      }
      modelCatalogCache.set(nid, { ts: Date.now(), keys });
      logger.debug('model.catalog_cached', { node: nid, count: keys.size });
    }
    const cur = modelCatalogCache.get(nid);
    if (!cur || !cur.keys.size) return true;
    return cur.keys.has(`${model.providerID}/${model.modelID}`) || cur.keys.has(model.modelID);
  } catch (_) {
    return true; // fail-open
  }
}

function proxyApi(req, res, node) {
  const startedAt = Date.now();
  const method = req.method;
  const url = req.url;
  const base = node && node.base ? node.base : `http://${API_HOST}:${API_PORT}`;
  const host = node && node.host ? node.host : API_HOST;
  const port = node && node.port ? node.port : API_PORT;

  const options = {
    hostname: host,
    port: port,
    path: url,
    method,
    headers: {
      'Authorization': AUTH,
      'Content-Type': req.headers['content-type'] || 'application/json',
      'Accept': req.headers['accept'] || '*/*'
    }
  };

  const preq = http.request(options, (pres) => {
    const isSSE = (pres.headers['content-type'] || '').includes('text/event-stream');
    const tapState = isSSE && url === '/event' ? { rem: '', dead: false, node } : null;
    if (tapState) res.on('close', () => { tapState.dead = true; });
    res.writeHead(pres.statusCode || 502, {
      'Content-Type': pres.headers['content-type'] || 'application/json',
      'Cache-Control': 'no-cache',
      'Connection': isSSE ? 'keep-alive' : 'close'
    });
    logger.info('proxy.response', {
      method, path: url, status: pres.statusCode || 502,
      durationMs: Date.now() - startedAt, sse: isSSE
    });
    // STREAMING: passa chunks direto, não bufferiza (e faz tap no /event)
    pres.on('data', (chunk) => {
      if (tapState) tapSseChunk(chunk, tapState);
      res.write(chunk);
    });
    pres.on('end', () => res.end());
    pres.on('error', (e) => {
      logger.error('proxy.upstream_error', { method, path: url, error: e.message });
      res.end();
    });
  });

  preq.on('error', (e) => {
    logger.error('proxy.connect_error', { method, path: url, error: e.message });
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'proxy: ' + e.message }));
    }
  });

  if (method === 'POST' || method === 'PUT') {
    // Se a rota já leu o body (ex: POST /session p/ parsear nodeId), reutiliza
    // o buffer em vez de re-ouvir o stream já consumido (senão envia body vazio).
    if (req._bufferedBody !== undefined) {
      const b = req._bufferedBody;
      logger.debug('proxy.request_body', { method, path: url, body: b.slice(0, 500) });
      preq.end(b);
      return;
    }
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      // loga o corpo truncado (evita despejar SSE/prompts gigantes)
      logger.debug('proxy.request_body', { method, path: url, body: body.slice(0, 500) });
      preq.end(body);
    });
  } else {
    preq.end();
  }
}

// Fan-in do /event global: antes só encaminhava o node `main`; agora abre
// uma conexão /event por node alcançável e mescla num único stream, taggeando
// cada evento com {node:{id,name,color}}. Reconexão por node com backoff.
const EVENT_RECONNECT_MIN_MS = parseInt(process.env.EVENT_RECONNECT_MIN_MS || '2000', 10);

function proxyEventFanIn(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  const clients = []; // { node, preq, pres, rem, dead, timer }
  let closed = false;

  const cleanup = () => {
    if (closed) return;
    closed = true;
    for (const c of clients) {
      if (c.timer) clearTimeout(c.timer);
      try { c.preq.destroy(); } catch (_) {}
      try { c.pres.destroy(); } catch (_) {}
    }
    clients.length = 0;
    try { res.end(); } catch (_) {}
  };
  res.on('close', cleanup);
  res.on('error', cleanup);

  const connectNode = (node) => {
    if (closed) return;
    if (isNodeDown(node.id)) {
      // node offline: agenda reconexão sem spammar
      const c = { node, preq: null, pres: null, rem: '', dead: false, timer: null };
      clients.push(c);
      c.timer = setTimeout(() => {
        const i = clients.indexOf(c);
        if (i >= 0) clients.splice(i, 1);
        connectNode(node);
      }, EVENT_RECONNECT_MIN_MS);
      return;
    }
    const base = node.base || `http://${node.host}:${node.port}`;
    const u = new URL(base);
    const options = {
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: '/event',
      method: 'GET',
      headers: { 'Authorization': AUTH, 'Accept': 'text/event-stream' }
    };
    const transport = u.protocol === 'https:' ? https : http;
    const preq = transport.request(options, (pres) => {
      const c = { node, preq, pres, rem: '', dead: false, timer: null };
      clients.push(c);
      pres.on('data', (chunk) => {
        if (closed) return;
        c.rem += chunk.toString('utf8');
        const lines = c.rem.split('\n');
        c.rem = lines.pop();
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          let evt;
          try { evt = JSON.parse(line.slice(6)); } catch { continue; }
          // taggeia a origem do node (não sobrescreve properties existentes)
          const props = evt.properties || {};
          if (!props.node) {
            props.node = { id: node.id, name: node.name, color: node.color };
          }
          const out = { ...evt, properties: props };
          try { res.write(`data: ${JSON.stringify(out)}\n\n`); } catch (_) { cleanup(); return; }
        }
      });
      pres.on('end', () => {
        const i = clients.indexOf(c);
        if (i >= 0) clients.splice(i, 1);
        if (!closed) {
          c.timer = setTimeout(() => connectNode(node), EVENT_RECONNECT_MIN_MS);
        }
      });
      pres.on('error', () => {
        const i = clients.indexOf(c);
        if (i >= 0) clients.splice(i, 1);
        if (!closed) {
          c.timer = setTimeout(() => connectNode(node), EVENT_RECONNECT_MIN_MS);
        }
      });
    });
    preq.on('error', () => {
      if (!closed) {
        const c = { node, preq, pres: null, rem: '', dead: false, timer: null };
        clients.push(c);
        c.timer = setTimeout(() => {
          const i = clients.indexOf(c);
          if (i >= 0) clients.splice(i, 1);
          connectNode(node);
        }, EVENT_RECONNECT_MIN_MS);
      }
    });
    preq.end();
  };

  // conecta em todos os nodes alcançáveis (main + lubuntu + futuros)
  for (const n of NODES) connectNode(n);
  logger.info('event.fan_in_started', { nodes: NODES.map((n) => n.id) });
}

// Guard anti-misroute de rotas per-session: sem node dono e sem bridge,
// o proxy NÃO pode cair silenciosamente no main (sessão de node offline
// apareceria vazia/errada no celular). Responde direto o erro operacional.
function replySessionMiss(res, code, msg) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  return res.end(JSON.stringify({ error: msg }));
}

// ============================================================
// HTTP server
// ============================================================
const requestHandler = async (req, res) => {
  const urlPath = req.url.split('?')[0];
  const method = req.method;

  if (/%7B|%7D|\{id\}|\{sessionId\}|\{sessionID\}/.test(req.url)) {
    logger.warn('proxy.template_path_suspect', {
      ip: req.socket.remoteAddress, method, path: urlPath
    });
  }

  // ---- GATE DE AUTH: rotas de dados exigem Basic igual ao upstream ----
  // (estáticos ficam abertos; API/event/session/config exigem credencial)
  const isApiRoute =
    urlPath.startsWith('/api/') ||
    urlPath.startsWith('/config') ||
    urlPath === '/event' ||
    urlPath === '/project' ||
    urlPath === '/permission' ||
    urlPath.startsWith('/permission/') ||
    urlPath === '/question' ||
    urlPath.startsWith('/question/') ||
    urlPath === '/session' ||
    urlPath.startsWith('/session/');
  if (isApiRoute && req.headers.authorization !== AUTH) {
    logger.warn('proxy.auth_denied', { ip: req.socket.remoteAddress, method, path: urlPath });
    res.writeHead(401, {
      'Content-Type': 'application/json; charset=utf-8',
      'WWW-Authenticate': 'Basic realm="opencode-pwa"',
    });
    return res.end(JSON.stringify({ _tag: 'UnauthorizedError', message: 'Authentication required' }));
  }

  // ---- BRIDGE: sessões TUI (não materializadas pelo serve) ----
  // ============================================================
  // GET /api/nodes — lista os nodes (PCs) registrados + health deles.
  // Cada node tem {id, name, color, host, port, health}. Cache por node.
  // ============================================================
  if (method === 'GET' && urlPath === '/api/nodes') {
    const serve = (code, obj) => {
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };
    const out = await Promise.all(NODES.map(async (n) => {
      try {
        const h = await nodeHealth(n, true);
        return { id: n.id, name: n.name, color: n.color, host: n.host, port: n.port, health: h, reachable: true };
      } catch (e) {
        // transição down já é logada por markNodeDown; aqui só debug por-request
        logger.debug('nodes.health_error', { node: n.id, error: e.message });
        return { id: n.id, name: n.name, color: n.color, host: n.host, port: n.port, health: null, reachable: false };
      }
    }));
    logger.info('nodes.list', { count: NODES.length, ids: NODES.map((n) => n.id).join(',') });
    return serve(200, { nodes: out });
  }

  // POST /api/nodes/:id — renomeia/recolore um nó. Persistido em
  // node-names.json (server-side), valendo para todos os clientes.
  if (method === 'POST' && urlPath.startsWith('/api/nodes/')) {
    const nid = decodeURIComponent(urlPath.slice('/api/nodes/'.length).split('/')[0]);
    const node = nodeById(nid);
    if (!node) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: `node desconhecido: ${nid}` }));
    }
    let body = '';
    req.on('data', (c) => body += c);
    req.on('end', () => {
      let name, color;
      try { ({ name, color } = JSON.parse(body || '{}')); } catch (_) { name = undefined; color = undefined; }
      if (name !== undefined) node.name = String(name).slice(0, 40);
      if (color !== undefined) node.color = String(color).slice(0, 20);
      nodeNames[nid] = { name: node.name, color: node.color };
      saveNodeNames();
      logger.info('node.renamed', { node: nid, name: node.name, color: node.color });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ id: nid, name: node.name, color: node.color }));
    });
    return;
  }

  // GET /api/node/:id/pc-health — telemetria de UM node (PC) p/ o app.
  // CPU% via delta de /proc/stat, temp x86_pkg_temp, RAM meminfo,
  // bateria power_supply. Cache por node (TTL 2s) pra aguentar poll do app.
  const nodeHealthMatch = urlPath.match(/^\/api\/node\/([^/]+)\/pc-health$/);
  if (method === 'GET' && nodeHealthMatch) {
    const serve = (code, obj) => {
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };
    const nid = decodeURIComponent(nodeHealthMatch[1]);
    const node = nodeById(nid);
    if (!node) {
      logger.warn('pc-health.unknown_node', { node: nid });
      return serve(404, { error: `node desconhecido: ${nid}` });
    }
    try {
      const d = await nodeHealth(node);
      return serve(200, d);
    } catch (e) {
      logger.debug('pc-health.error', { node: nid, error: e.message });
      return serve(502, { error: e.message });
    }
  }

  const liveMatch = urlPath.match(/^\/api\/session\/([^/]+)\/live$/);
  if (method === 'GET' && liveMatch) {
    const sid = decodeURIComponent(liveMatch[1]);
    const out = [];
    for (const [id, v] of liveParts) {
      if (v.sessionId === sid) out.push({ id, text: v.text, reasoning: v.reasoning });
    }
    let curModel = sessionModels.get(sid) || null;
    if (!curModel) {
      const node = await findSessionNode(sid);
      const base = node ? node.base : `http://${API_HOST}:${API_PORT}`;
      fetch(`${base}/session/${encodeURIComponent(sid)}`, { headers: { Authorization: AUTH } })
        .then((r) => (r.ok ? r.json() : null))
        .then((sj) => {
          const m = sj && sj.model;
          if (m && m.providerID && (m.id || m.modelID)) {
            sessionModels.set(sid, { providerID: m.providerID, modelID: m.id || m.modelID });
          }
        })
        .catch(() => {});
    }
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ data: out, model: curModel }));
  }

  if (method === 'GET' && urlPath === '/api/session') {
    return proxySessionsSorted(req, res);
  }

  const msgMatch = urlPath.match(/^\/api\/session\/([^/]+)\/message$/);
  if (method === 'GET' && msgMatch) {
    const sessionId = decodeURIComponent(msgMatch[1]);
    const node = await findSessionNode(sessionId);
    if (bridgeHasMessages(sessionId)) {
      const limit = parseInt(new URL(req.url, 'http://x').searchParams.get('limit') || '100', 10);
      const synth = bridgeMessages(sessionId, limit);
      if (synth) {
        logger.info('bridge.message_synthesized', { sessionId: sessionId.slice(0, 12), count: synth.data.length, node: node ? node.id : 'main' });
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(synth));
      }
    }
    if (!node) return replySessionMiss(res, 404, 'sessão não encontrada em nenhum node');
    return proxyApi(req, res, node);
  }

  const evtMatch = urlPath.match(/^\/api\/session\/([^/]+)\/event$/);
  if (method === 'GET' && evtMatch) {
    const sessionId = decodeURIComponent(evtMatch[1]);
    const node = await findSessionNode(sessionId);
    if (bridgeHasMessages(sessionId)) {
      logger.info('bridge.sse_synthesized', { sessionId: sessionId.slice(0, 12), node: node ? node.id : 'main' });
      return bridgeSSE(req, res, sessionId);
    }
    if (!node) return replySessionMiss(res, 404, 'sessão não encontrada em nenhum node');
    return proxyApi(req, res, node);
  }

  // ============================================================
  // PROMPT — encaminha ao endpoint oficial do serve.
  // Inserir direto em session_input NÃO funciona p/ sessão ociosa:
  // a fila só é drenada dentro de uma run ativa (runner/llm.ts).
  //
  // Descobertas 24/08 (validadas E2E):
  // - O plugin oh-my-openagent SOBRESCREVE o modelo do request e do
  //   pin de sessão; chains de agentes builtin resolvem p/ ids mortos
  //   do catálogo zen (opencode/kimi-k3 etc.) -> ProviderModelNotFoundError.
  // - agent 'Sisyphus-Junior' (chain c/ modelo free do 9router) roda OK
  //   em sessões criadas pela TUI e pelo serve.
  // - O serve pode responder 200 com HTML (catch-all de rota inexistente)
  //   e 500 com corpo vazio na prática (run morre sem parts) — então
  //   status HTTP sozinho NÃO é sucesso; validamos o corpo.
  // ============================================================
  const promptMatch = urlPath.match(/^\/api\/session\/([^/]+)\/prompt$/);
  if (method === 'POST' && promptMatch) {
    const sessionId = decodeURIComponent(promptMatch[1]);
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      let promptText = '';
      let wantedModel = null;
      let wantedAgent = null;
      let imageDataUrl = null;
      try {
        const j = JSON.parse(body);
        promptText = j.prompt?.text || j.text || '';
        if (j.model?.providerID && j.model?.modelID) {
          // IMPORTANTE: manter modelID E id — modelExists() valida
          // `${providerID}/${modelID}`; sem modelID o modelo escolhido vira
          // "opencode/undefined" e é sempre descartado (cai pra last_good).
          wantedModel = { providerID: j.model.providerID, id: j.model.modelID, modelID: j.model.modelID };
        }
        if (typeof j.agent === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(j.agent)) {
          wantedAgent = j.agent;
        }
        if (j.image && typeof j.image.data === 'string' && j.image.data) {
          const mediaType = typeof j.image.mediaType === 'string' && j.image.mediaType ? j.image.mediaType : 'image/jpeg';
          imageDataUrl = `data:${mediaType};base64,${j.image.data}`;
        }
      } catch (_) {}
      if (!promptText && !imageDataUrl) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'prompt.text ausente' }));
      }
      // imagem anexada: descreve com o modelo de visão e injeta a descrição
      // no prompt do modelo principal (arquitetura: modelo principal -> visão
      // descreve -> output consolidado). Falha de visão não bloqueia o envio.
      if (imageDataUrl) {
        const desc = await describeImage(imageDataUrl);
        if (desc) {
          promptText = `[Imagem anexada — descrição gerada por modelo de visão]\n${desc}\n\n${promptText}`.trim();
          logger.info('prompt.image_injected', { sessionIdFull: sessionId, descChars: desc.length });
        } else {
          promptText = `[Imagem anexada — não foi possível descrever]\n\n${promptText}`.trim();
        }
      }

      const node = await findSessionNode(sessionId);
      if (!node) return replySessionMiss(res, 502, 'node da sessão offline ou sessão não encontrada');
      const base = node ? node.base : `http://${API_HOST}:${API_PORT}`;
      if (node && node.id !== 'main') logger.info('prompt.routed_to_node', { sessionIdFull: sessionId, node: node.id, base });
      const authHeaders = { Authorization: AUTH, 'Content-Type': 'application/json' };

      (async () => {
        try {
          const sessRes = await fetch(`${base}/session/${encodeURIComponent(sessionId)}`, { headers: { Authorization: AUTH } });
          if (!sessRes.ok) {
            const msg = sessRes.status === 404 ? 'sessão não encontrada no serve' : `serve respondeu ${sessRes.status}`;
            logger.warn('prompt.session_invalid', { sessionIdFull: sessionId, status: sessRes.status });
            res.writeHead(sessRes.status === 404 ? 404 : 502, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ error: msg }));
          }

          let model = wantedModel;
          if (!model) {
            // 1º: herda o modelo atual/pinado da sessão (igual ao TUI)
            try {
              const sr = await fetch(`${base}/session/${encodeURIComponent(sessionId)}`, { headers: { Authorization: AUTH } });
              if (sr.ok) {
                const sm = (await sr.json())?.model;
                if (sm?.providerID && (sm.id || sm.modelID)) {
                  model = { providerID: sm.providerID, modelID: sm.id || sm.modelID };
                  logger.info('prompt.model_selected', { sessionIdFull: sessionId, source: 'session', model: `${model.providerID}/${model.modelID}` });
                }
              }
            } catch (_) {}
          }
          // valida contra o catálogo: pin morto (ex.: opencode/undefined) não
          // pode matar o run — cai pro último modelo que funcionou NESSA sessão
          if (model) {
            const known = await modelExists(model, node);
            if (!known) {
              logger.warn('prompt.model_invalid_fallback', {
                sessionIdFull: sessionId,
                dead: `${model.providerID}/${model.modelID}`,
                node: node ? node.id : 'main',
              });
              model = null;
            }
          }
          if (!model) {
            const good = sessionGoodModel.get(sessionId);
            if (good && (await modelExists(good, node))) {
              model = good;
              logger.info('prompt.model_selected', { sessionIdFull: sessionId, source: 'last_good', model: `${model.providerID}/${model.modelID}`, node: node ? node.id : 'main' });
            }
          }
          if (!model) {
            model = parseModelRef(PHONE_DEFAULT_MODEL);
            logger.info('prompt.model_selected', { sessionIdFull: sessionId, source: 'default', model: `${model.providerID}/${model.modelID}` });
          }

          if (model) {
            const sm = await fetch(`${base}/api/session/${encodeURIComponent(sessionId)}/model`, {
              method: 'POST',
              headers: { ...authHeaders, Connection: 'close' },
              body: JSON.stringify({ model: { providerID: model.providerID, id: model.modelID || model.id } })
            });
            logger.info('prompt.switch_model', { status: sm.status, model });
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          // agent builtin ('build'/'plan') → chain do plugin resolve ids mortos
          // do catálogo zen (ProviderModelNotFoundError, ver 24/08). Só
          // Sisyphus-Junior roda OK — força PHONE_AGENT pro forward.
          const useAgent = (wantedAgent && wantedAgent !== 'build' && wantedAgent !== 'plan') ? wantedAgent : PHONE_AGENT;
          if (wantedAgent && wantedAgent !== useAgent) {
            logger.warn('prompt.agent_forced', { sessionIdFull: sessionId, wantedAgent, forced: useAgent });
          }
          res.end(JSON.stringify({
            data: { id: 'msg_fwd_' + Date.now().toString(36), sessionID: sessionId, accepted: true },
            agent: useAgent,
            model
          }));

          const payload = JSON.stringify({
            parts: [{ type: 'text', text: promptText }],
            agent: useAgent,
            model: model ? { providerID: model.providerID, modelID: model.modelID || model.id } : undefined
          });
          // SEM retry automático: um reenvio pode DUPLICAR o prompt no serve
          // (aceito lá mas resposta perdida aqui = mensagem entrando sozinha)
          try {
            const up = await fetch(`${base}/session/${encodeURIComponent(sessionId)}/message`, {
              method: 'POST',
              headers: { ...authHeaders, Connection: 'close' },
              body: payload
            });
            const upBody = await up.text();
            let ok = up.ok;
            let parsedInfo = null;
            try {
              const pj = JSON.parse(upBody);
              parsedInfo = pj?.info || pj?.data?.info || null;
            } catch (_) { ok = false; }
            if (upBody.trimStart().startsWith('<')) ok = false;
            if (ok) {
              if (model) sessionGoodModel.set(sessionId, model);
              logger.info('prompt.forwarded', {
                sessionIdFull: sessionId, model, status: up.status,
                assistantMessageID: parsedInfo?.id || null
              });
            } else {
              logger.error('prompt.forward_failed', {
                sessionIdFull: sessionId, model, agent: PHONE_AGENT,
                status: up.status, resp: upBody.slice(0, 300)
              });
              ntfyPush('❌ Prompt do celular falhou', `${up.status}: ${upBody.slice(0, 140).replace(/\n/g, ' ')}`, ['warning']);
            }
          } catch (e) {
            logger.error('prompt.forward_error', { error: e.message });
            ntfyPush('❌ Prompt do celular falhou', e.message, ['warning']);
          }
        } catch (e) {
          logger.error('prompt.forward_error', { error: e.message });
          ntfyPush('❌ Prompt do celular falhou', e.message, ['warning']);
        }
      })();
    });
    return;
  }

  // REWRITE+ABORT: APKs antigos chamam /api/session/:id/abort; a rota real do
  // serve é /session/:id/abort — sem isto o botão parar não aborta (200 falso).
  // Em ambos os caminhos, limpa liveParts da sessão pra /live não realimentar
  // a UI com a mensagem já morta (spinner eterno após parar)

  // POST /session — cria uma sessão NO NÓ ESCOLHIDO. O app envia { nodeId } no
  // body; sem nodeId cria no default (main). Roteia o forward pro nó dono.
  if (method === 'POST' && urlPath === '/session') {
    let body = '';
    req.on('data', (c) => body += c);
    req.on('end', () => {
      let nodeId, payload;
      try {
        const j = JSON.parse(body || '{}');
        nodeId = j.nodeId;
        payload = { ...j };
        delete payload.nodeId;
      } catch (_) { nodeId = undefined; payload = undefined; }
      const node = nodeId ? nodeById(nodeId) : null;
      if (nodeId && !node) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: `node desconhecido: ${nodeId}` }));
      }
      logger.info('session.create', { node: node ? node.id : 'main', payload: payload ? Object.keys(payload) : [] });
      req.url = '/session';
      req._bufferedBody = body; // proxyApi reutiliza (evita re-ouvir stream consumido)
      return proxyApi(req, res, node);
    });
    return;
  }

  if (method === 'POST') {
    const am = urlPath.match(/^\/api\/session\/([^/]+)\/abort$/) || urlPath.match(/^\/session\/([^/]+)\/abort$/);
    if (am) {
      const sid = decodeURIComponent(am[1]);
      req.url = `/session/${encodeURIComponent(sid)}/abort`;
      for (const [k, v] of liveParts) if (v.sessionId === sid) liveParts.delete(k);
      const node = await findSessionNode(sid);
      if (!node) return replySessionMiss(res, 502, 'node da sessão offline ou sessão não encontrada');
      if (node && node.id !== 'main') logger.info('abort.routed_to_node', { sessionId: sid.slice(0, 14), node: node.id });
      logger.info('abort.forward', { sessionId: sid.slice(0, 14), node: node ? node.id : 'main' });
      return proxyApi(req, res, node);
    }
  }

  // API paths -> forward para a API real (com streaming para SSE)
  if (urlPath === '/project' || urlPath === '/session' || urlPath.startsWith('/session/') ||
      urlPath === '/permission' || urlPath.startsWith('/permission/') ||
      urlPath === '/question' || urlPath.startsWith('/question/') ||
      urlPath.startsWith('/api/') || urlPath.startsWith('/config')) {
    logger.debug('proxy.request', { method, path: urlPath });
    return proxyApi(req, res);
  }

  // /event global: fan-in multi-nó (main + lubuntu + futuros) — o celular
  // vê a atividade ao vivo de TODOS os PCs, não só do main.
  if (urlPath === '/event') {
    logger.debug('proxy.event_fan_in', { method, path: urlPath });
    return proxyEventFanIn(req, res);
  }

  // Static files (contenção de path — bloqueia traversal tipo /../)
  let rel = urlPath === '/' ? 'index.html' : decodeURIComponent(urlPath);
  const filePath = path.resolve(__dirname, '.' + path.sep + rel.replace(/^([/\\])+/, ''));
  if (!filePath.startsWith(__dirname + path.sep)) {
    logger.warn('static.traversal_blocked', { method, path: urlPath, ip: req.socket.remoteAddress });
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    return res.end('Forbidden');
  }
  let extname = path.extname(filePath);
  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        logger.warn('static.not_found', { method, path: urlPath });
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
      } else {
        logger.error('static.read_error', { method, path: urlPath, error: err.message });
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Server Error');
      }
    } else {
      logger.debug('static.served', { method, path: urlPath, bytes: content.length });
      // Injeta a auth Basic no app.js (placeholder __APP_AUTH__) para que o
      // frontend autentique no proxy sem expor credenciais hardcoded no fonte.
      if (rel === '/app.js') {
        content = Buffer.from(content.toString('utf8').split('__APP_AUTH__').join(AUTH));
      }
      res.writeHead(200, { 'Content-Type': MIME[extname] || 'application/octet-stream' });
      res.end(content);
    }
  });
};

const server = USE_HTTPS
  ? https.createServer({ key: fs.readFileSync(HTTPS_KEY), cert: fs.readFileSync(HTTPS_CERT) }, requestHandler)
  : http.createServer(requestHandler);

server.listen(PORT, '0.0.0.0', () => {
  logger.info('server.start', {
    port: PORT, api: `${API_HOST}:${API_PORT}`, pid: process.pid, https: USE_HTTPS,
    nodes: NODES.map((n) => ({ id: n.id, name: n.name, color: n.color, base: n.base })),
  });
  console.log(`PWA + proxy rodando em ${USE_HTTPS ? 'https' : 'http'}://0.0.0.0:${PORT} (API -> ${API_HOST}:${API_PORT}, SSE streaming habilitado)`);
  console.log(`Nodes registrados: ${NODES.map((n) => `${n.id}@${n.base}`).join('  ')}`);
  console.log(`Logs: ${LOG_FILE}`);
});
