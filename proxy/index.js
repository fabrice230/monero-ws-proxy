// Proxy WebSocket -> Stratum TCP optimisé pour Fly.io / Render
const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('../vendor/ws');
const config = require('../config');

const MIME = {
  '.html':  'text/html',
  '.js':    'application/javascript',
  '.wasm':  'application/wasm',
  '.css':   'text/css',
  '.woff2': 'font/woff2',
};

const publicDir = path.join(__dirname, '..', 'public');

function hexToBytes(hex) {
  const out = Buffer.alloc(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
  return out;
}

function targetToDiff(targetHex) {
  const raw = hexToBytes(targetHex);
  let target = 0n;
  if (raw.length === 4) {
    const t32 = BigInt(raw.readUInt32LE(0));
    if (t32 !== 0n) target = 0xffffffffffffffffn / (0xffffffffn / t32);
  } else if (raw.length === 8) {
    target = raw.readBigUInt64LE(0);
  } else if (raw.length >= 32) {
    target = raw.readBigUInt64LE(24);
  }
  return target ? Number(0xffffffffffffffffn / target) : 0;
}

function createSession(tag, sendToClient, target = {}) {
  let pool           = null;
  let poolBuffer     = '';
  let poolReady      = false;
  let minerId        = null;
  let pending        = [];
  let rewriteLogin   = true;
  const sessionUid   = Math.random().toString(36).slice(2, 8);

  let currentHost    = target.host || config.POOL_HOST || 'gulf.moneroocean.stream';
  let currentPort    = target.port || config.POOL_PORT || 10128;
  let walletOverride = null;
  let workerOverride = null;

  function sendToPool(msg) {
    if (!pool) connectPool();
    const line = JSON.stringify(msg) + '\n';
    if (poolReady && pool && !pool.destroyed) pool.write(line);
    else pending.push(line);
  }

  function safeSendToClient(obj) {
    try { sendToClient(JSON.stringify(obj)); } catch (_) {}
  }

  function disconnectPool() {
    if (pool && !pool.destroyed) { try { pool.destroy(); } catch (_) {} }
    pool = null;
    poolReady = false;
    poolBuffer = '';
  }

  function connectPool() {
    const myPool = new net.Socket();
    myPool.setNoDelay(true);
    myPool.setKeepAlive(true, 15000);
    pool = myPool;
    poolBuffer = '';

    myPool.connect(currentPort, currentHost, () => {
      if (pool !== myPool) return;
      console.log(`[${tag}/pool] connected ${currentHost}:${currentPort}`);
      poolReady = true;
      for (const line of pending) myPool.write(line);
      pending = [];
    });

    myPool.on('data', (data) => {
      if (pool !== myPool) return;
      poolBuffer += data.toString();
      const lines = poolBuffer.split('\n');
      poolBuffer = lines.pop();
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const msg = JSON.parse(line);
          if (msg.result && msg.result.id) minerId = msg.result.id;
          const job = (msg.result && msg.result.job) || (msg.method === 'job' && msg.params);
          if (job && job.target) {
            const diff = targetToDiff(job.target);
            console.log(`[${tag}/pool job] id ${job.job_id} diff ${diff || '?'}`);
          }
          safeSendToClient(msg);
        } catch (_) {
          console.error(`[${tag}/pool] bad JSON: ${line.slice(0, 80)}`);
        }
      }
    });

    myPool.on('error', (err) => {
      if (pool !== myPool) return;
      console.error(`[${tag}/pool] error: ${err.message}`);
      poolReady = false;
      safeSendToClient({ error: 'Pool connection error: ' + err.message });
    });

    myPool.on('close', () => {
      if (pool !== myPool) return;
      console.log(`[${tag}/pool] disconnected`);
      poolReady = false;
      safeSendToClient({ error: 'Pool disconnected' });
    });
  }

  function onClientMessage(rawText) {
    try {
      const msg = JSON.parse(rawText);

      if (msg.method === 'ping') {
        safeSendToClient({ id: msg.id, result: { status: 'OK' } });
        return;
      }

      if (msg.method === 'set_target') {
        const p = msg.params || {};
        const newHost   = p.host   ? String(p.host) : currentHost;
        const newPort   = p.port   ? Number(p.port) : currentPort;
        const newWallet = p.wallet ? String(p.wallet) : walletOverride;
        const newWorker = (typeof p.worker === 'string') ? p.worker : workerOverride;
        const changed   = newHost !== currentHost || newPort !== currentPort;
        currentHost = newHost;
        currentPort = newPort;
        walletOverride = newWallet;
        workerOverride = newWorker;
        console.log(`[${tag}/set_target] -> ${currentHost}:${currentPort}, wallet=${(walletOverride || config.WALLET || '').slice(0, 12)}…`);
        if (changed) {
          const wasConnected = !!pool;
          disconnectPool();
          pending = [];
          if (wasConnected) connectPool();
        }
        return;
      }

      if (msg.method === 'login' && rewriteLogin) {
        msg.params = msg.params || {};
        const baseLogin  = walletOverride || config.WALLET;
        const baseWorker = (workerOverride && workerOverride.trim()) || config.WORKER_NAME || 'x';
        const uniqueWorker = `${baseWorker}-${sessionUid}`;
        msg.params.login = baseLogin + (config.FIXED_DIFF ? '+' + config.FIXED_DIFF : '');
        msg.params.pass  = uniqueWorker;
        msg.params.rigid = uniqueWorker;
        msg.params.agent = `gh-distro-miner/1.0 (${tag})`;
        msg.params.algo  = ['rx/0'];
        console.log(`[${tag}/login] worker=${uniqueWorker}`);
      }

      if (msg.method === 'submit' && minerId && rewriteLogin) {
        msg.params = msg.params || {};
        msg.params.id = minerId;
      }

      sendToPool(msg);
    } catch (e) {
      console.error(`[${tag}] bad client message: ${e.message}`);
    }
  }

  function onClientClose() {
    console.log(`[${tag}] client disconnected`);
    if (pool) pool.destroy();
  }

  return {
    onClientMessage,
    onClientClose,
    setLoginPassthrough() { rewriteLogin = false; },
  };
}

const requestHandler = (req, res) => {
  // Healthcheck explicite pour Fly.io / Render (évite le redémarrage du conteneur)
  if (req.url === '/health' || req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('OK');
    return;
  }

  // Support CORS preflight pour les requêtes externes
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': '*',
    });
    res.end();
    return;
  }

  let url;
  try { url = new URL(req.url, 'http://localhost'); }
  catch { res.writeHead(400); res.end('bad request'); return; }

  const relative = url.pathname === '/' ? 'index.html' : url.pathname.replace(/^\/+/, '');
  const filePath = path.resolve(publicDir, relative);
  if (!filePath.startsWith(publicDir + path.sep) && filePath !== publicDir) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('Monero WS-Stratum Proxy Running');
    return;
  }

  const ext = path.extname(filePath);
  res.writeHead(200, {
    'Content-Type': MIME[ext] || 'application/octet-stream',
    'Cross-Origin-Opener-Policy':   'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': ext === '.html' ? 'same-origin' : 'cross-origin',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-store',
  });

  if (/\.html$/i.test(filePath)) {
    let html = fs.readFileSync(filePath, 'utf8');
    html = html
      .replace(/DEFAULT_WALLET_PLACEHOLDER/g,    config.WALLET    || '')
      .replace(/DEFAULT_POOL_HOST_PLACEHOLDER/g, config.POOL_HOST || '')
      .replace(/DEFAULT_POOL_PORT_PLACEHOLDER/g, String(config.POOL_PORT || ''));
    res.end(html);
    return;
  }

  fs.createReadStream(filePath).pipe(res);
};

const server = http.createServer(requestHandler);
server.keepAliveTimeout = 65000;
server.headersTimeout   = 66000;
server.on('connection', (s) => { s.setNoDelay(true); s.setKeepAlive(true, 15000); });

// Accepte toutes les origines (widget embarqué sur n'importe quel domaine)
const wss = new WebSocketServer({
  server,
  perMessageDeflate: false,
  verifyClient: () => true,
});

wss.on('connection', (ws, req) => {
  console.log(`[ws] browser connected from ${req.headers.origin || 'unknown origin'}`);
  const url = new URL(req.url, 'http://localhost');
  const routed = url.searchParams.has('pool');
  const host = url.searchParams.get('pool');
  const port = Number(url.searchParams.get('port'));

  if (routed && (!host || host.length > 253 || /[\s/<>]/.test(host) || !Number.isInteger(port) || port < 1 || port > 65535)) {
    ws.close(1008, 'Invalid pool target');
    return;
  }

  const session = createSession('ws', (text) => {
    if (ws.readyState === 1) { // WebSocket.OPEN
      ws.send(text, { compress: false });
    }
  }, routed ? { host, port } : {});

  if (routed) session.setLoginPassthrough();

  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', (err) => {
    console.error('[ws] socket error:', err.message);
  });
  ws.on('message', (data) => session.onClientMessage(data.toString()));
  ws.on('close', (code, reason) => {
    console.log(`[ws] closed code=${code} reason=${reason || 'none'}`);
    session.onClientClose();
  });
});

// Heartbeat toutes les 25s (évite que Fly.io coupe la connexion pour inactivité)
const wsHeartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      ws.close(1001, 'Ping timeout');
      continue;
    }
    ws.alive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 25000);
wss.on('close', () => clearInterval(wsHeartbeat));

// Démarrage optionnel du bridge TCP brut (uniquement si un port est configuré)
if (config.STRATUM_TCP_PORT) {
  const tcpServer = net.createServer((socket) => {
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 15000);
    console.log(`[tcp] stratum client ${socket.remoteAddress}:${socket.remotePort}`);
    const session = createSession('tcp', (text) => {
      if (!socket.destroyed) socket.write(text + '\n');
    });
    session.setLoginPassthrough();

    let buf = '';
    socket.on('data', (chunk) => {
      buf += chunk.toString();
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) if (line.trim()) session.onClientMessage(line);
    });
    socket.on('close', () => session.onClientClose());
    socket.on('error', (err) => console.error('[tcp] socket error:', err.message));
  });

  tcpServer.on('error', (err) => console.error('[tcp] server error:', err.message));
  tcpServer.listen(config.STRATUM_TCP_PORT, '0.0.0.0', () => {
    console.log(`[tcp] stratum bridge on tcp://0.0.0.0:${config.STRATUM_TCP_PORT}`);
  });
}

// Écoute impérative sur 0.0.0.0 et process.env.PORT (requis par Fly.io / Render)
const LISTEN_PORT = Number(process.env.PORT || config.WS_PORT || 8080);
server.listen(LISTEN_PORT, '0.0.0.0', () => {
  console.log(`RandomX bonanza proxy listening on 0.0.0.0:${LISTEN_PORT}`);
  console.log(`default pool: ${config.POOL_HOST}:${config.POOL_PORT}`);
});