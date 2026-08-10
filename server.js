/**
 * opencode-pwa proxy server
 *
 * - Serve os estáticos do PWA (index.html, app.js, style.css, ...)
 * - Faz proxy para a API real do opencode (porta 4333) com streaming (SSE)
 * - Injeção de auth Basic via auth.local.json (GITIGNORED) ou env vars
 * - Logging estruturado JSON em logs/server.log (níveis: debug|info|warn|error)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');

const API_HOST = process.env.API_HOST || '127.0.0.1';
const API_PORT = parseInt(process.env.API_PORT || '4333', 10);
const PORT = parseInt(process.env.PORT || '4335', 10);

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
const AUTH = (() => {
  const { username, password } = loadAuth();
  return 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
})();

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
function proxyApi(req, res) {
  const startedAt = Date.now();
  const method = req.method;
  const url = req.url;

  const options = {
    hostname: API_HOST,
    port: API_PORT,
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
    res.writeHead(pres.statusCode || 502, {
      'Content-Type': pres.headers['content-type'] || 'application/json',
      'Cache-Control': 'no-cache',
      'Connection': isSSE ? 'keep-alive' : 'close'
    });
    logger.info('proxy.response', {
      method, path: url, status: pres.statusCode || 502,
      durationMs: Date.now() - startedAt, sse: isSSE
    });
    // STREAMING: passa chunks direto, não bufferiza
    pres.on('data', (chunk) => res.write(chunk));
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

// ============================================================
// HTTP server
// ============================================================
const server = http.createServer((req, res) => {
  const urlPath = req.url.split('?')[0];
  const method = req.method;

  // API paths -> forward para a API real (com streaming para SSE)
  if (urlPath === '/project' || urlPath === '/session' ||
      urlPath.startsWith('/api/') || urlPath.startsWith('/config')) {
    logger.debug('proxy.request', { method, path: urlPath });
    return proxyApi(req, res);
  }

  // Static files
  let filePath = path.join(__dirname, urlPath === '/' ? 'index.html' : urlPath);
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
      res.writeHead(200, { 'Content-Type': MIME[extname] || 'application/octet-stream' });
      res.end(content);
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  logger.info('server.start', {
    port: PORT, api: `${API_HOST}:${API_PORT}`, pid: process.pid
  });
  console.log(`PWA + proxy rodando em http://0.0.0.0:${PORT} (API -> ${API_HOST}:${API_PORT}, SSE streaming habilitado)`);
  console.log(`Logs: ${LOG_FILE}`);
});
