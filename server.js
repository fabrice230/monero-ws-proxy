const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 8080;
const POOL_HOST = 'gulf.moneroocean.stream';
const POOL_PORT = 10128;

const wss = new WebSocket.Server({ port: PORT }, () => {
    console.log(`[PROXY] Serveur proxy en ligne et à l'écoute sur le port ${PORT}`);
});

wss.on('connection', (ws, req) => {
    console.log(`[CLIENT] Nouvelle connexion WebSocket établie.`);

    const tcpClient = new net.Socket();
    
    tcpClient.connect(POOL_PORT, POOL_HOST, () => {
        console.log(`[TCP] Connecté au pool distant ${POOL_HOST}:${POOL_PORT}`);
    });

    ws.on('message', (message) => {
        console.log(`[WS -> TCP] Relais vers le pool : ${message.toString().trim()}`);
        tcpClient.write(message);
    });

    tcpClient.on('data', (data) => {
        const response = data.toString();
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(response);
        }
    });

    tcpClient.on('error', (err) => {
        console.error(`[TCP ERROR] ${err.message}`);
    });

    ws.on('error', (err) => {
        console.error(`[WS ERROR] ${err.message}`);
    });

    tcpClient.on('close', () => {
        if (ws.readyState === WebSocket.OPEN) ws.close();
    });

    ws.on('close', () => {
        tcpClient.destroy();
    });
});