const http = require("http");
const net = require("net");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const POOL_HOST = process.env.POOL_HOST || "gulf.moneroocean.stream";
const POOL_PORT = Number(process.env.POOL_PORT) || 10128;

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({
      status: "ok",
      service: "monero-ws-proxy",
      pool: `${POOL_HOST}:${POOL_PORT}`,
      uptime: Math.floor(process.uptime())
    }));
    return;
  }
  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

const wss = new WebSocket.Server({
  server,
  path: "/",
  maxPayload: 1024 * 1024
});

wss.on("connection", (ws, req) => {
  const remote = req.socket.remoteAddress || "unknown";
  console.log(`[WS] Client connecté : ${remote}`);

  const tcp = net.createConnection({ host: POOL_HOST, port: POOL_PORT });
  let tcpBuffer = "";
  let closed = false;

  const closeBoth = (code = 1011, reason = "Upstream connection closed") => {
    if (closed) return;
    closed = true;
    if (ws.readyState === WebSocket.OPEN) ws.close(code, reason);
    tcp.destroy();
  };

  tcp.setTimeout(20000);
  tcp.on("connect", () => {
    console.log(`[TCP] Connecté à ${POOL_HOST}:${POOL_PORT}`);
    tcp.setTimeout(0);
  });

  tcp.on("data", chunk => {
    if (ws.readyState !== WebSocket.OPEN) return;
    // Le protocole Stratum utilise des messages JSON terminés par un saut de ligne.
    tcpBuffer += chunk.toString("utf8");
    if (tcpBuffer.length > 2 * 1024 * 1024) {
      console.error("[TCP] Tampon trop volumineux, fermeture.");
      closeBoth(1009, "Upstream buffer too large");
      return;
    }
    // Le proxy retransmet le flux reçu. Le client découpe les lignes JSON.
    ws.send(chunk, { binary: false }, err => {
      if (err) console.error(`[WS SEND] ${err.message}`);
    });
  });

  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      closeBoth(1003, "Text frames only");
      return;
    }
    if (!tcp.destroyed && tcp.writable) {
      tcp.write(data, err => {
        if (err) console.error(`[TCP WRITE] ${err.message}`);
      });
    }
  });

  tcp.on("timeout", () => {
    console.error("[TCP] Délai de connexion dépassé.");
    closeBoth(1011, "Pool connection timeout");
  });
  tcp.on("error", err => {
    console.error(`[TCP ERROR] ${err.message}`);
    closeBoth(1011, "Pool connection error");
  });
  tcp.on("close", () => {
    console.log("[TCP] Connexion pool fermée.");
    if (!closed && ws.readyState === WebSocket.OPEN) ws.close(1011, "Pool disconnected");
  });
  ws.on("error", err => {
    console.error(`[WS ERROR] ${err.message}`);
    tcp.destroy();
  });
  ws.on("close", () => {
    closed = true;
    tcp.destroy();
    console.log("[WS] Client déconnecté.");
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[HTTP] Proxy prêt sur le port ${PORT}`);
  console.log(`[POOL] Cible : ${POOL_HOST}:${POOL_PORT}`);
});

process.on("SIGTERM", () => {
  console.log("[SYSTEM] SIGTERM reçu, arrêt propre.");
  wss.close();
  server.close(() => process.exit(0));
});
