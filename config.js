
module.exports = {
  // Configuration du mineur
  WALLET: process.env.WALLET ||
    '4A8xTBWQKjdbzBHko5Tam8Yx4v3DenbkKg4ioQUAHYVUQzgH2Ji76rZHqp5hs47jVf56ijE6X3mYo2RtGNn84jvXCdz4Gwb',

  POOL_HOST: process.env.POOL_HOST || 'gulf.moneroocean.stream',
  POOL_PORT: Number(process.env.POOL_PORT || 10128),

  WORKER_NAME: process.env.WORKER_NAME || 'fabros',

  // Difficulté fixe
  FIXED_DIFF: Number(process.env.FIXED_DIFF || 9000),

  // Port HTTP/WebSocket
  WS_PORT: Number(process.env.WS_PORT || 8080),

  // Port TCP classique (non utilisé sur Render)
  STRATUM_TCP_PORT: Number(process.env.STRATUM_TCP_PORT || 8081),
};
