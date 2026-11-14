const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

const PORT = process.env.PORT || 10000;
const POOL_HOST = 'gulf.moneroocean.stream';
const POOL_PORT = 10128;

// Serveur HTTP de base pour le health check de Render (évite les erreurs de déploiement)
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Monero WebSocket Proxy is running!\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('[PROXY] Client WebSocket connecté.');

    const tcpSocket = new net.Socket();
    let buffer = '';

    tcpSocket.connect(POOL_PORT, POOL_HOST, () => {
        console.log(`[PROXY] Connecté au pool TCP ${POOL_HOST}:${POOL_PORT}`);
    });

    // Données reçues du pool TCP -> envoyées au WebSocket client
    tcpSocket.on('data', (data) => {
        buffer += data.toString('utf8');
        let lines = buffer.split('\n');
        buffer = lines.pop(); // Conserver le fragment de ligne incomplet dans le buffer

        for (let line of lines) {
            if (line.trim()) {
                if (ws.readyState === ws.OPEN) {
                    ws.send(line.trim() + '\n');
                }
            }
        }
    });

    // Messages reçus du WebSocket client -> envoyés au pool TCP
    ws.on('message', (message) => {
        const msgStr = message.toString().trim();
        if (msgStr) {
            // Ignore le keep-alive interne du client (id: 0) pour ne pas polluer le pool
            try {
                const parsed = JSON.parse(msgStr);
                if (parsed.id === 0) return;
            } catch (e) {}

            tcpSocket.write(msgStr + '\n');
        }
    });

    tcpSocket.on('error', (err) => {
        console.error('[PROXY] Erreur socket TCP pool:', err.message);
        if (ws.readyState === ws.OPEN) {
            ws.send(JSON.stringify({ error: { code: -1, message: 'TCP Pool Error: ' + err.message } }) + '\n');
        }
    });

    tcpSocket.on('close', () => {
        console.log('[PROXY] Connexion TCP pool fermée.');
        if (ws.readyState === ws.OPEN) {
            ws.close();
        }
    });

    ws.on('close', () => {
        console.log('[PROXY] Client WebSocket déconnecté.');
        tcpSocket.destroy();
    });

    ws.on('error', (err) => {
        console.error('[PROXY] Erreur WebSocket:', err.message);
        tcpSocket.destroy();
    });
});

server.listen(PORT, () => {
    console.log(`[PROXY] Serveur démarré sur le port ${PORT}`);
});
