// Defaults for the local demo proxy. The webui lets the user override
// wallet + pool at runtime; these are just the fallback values used when
// the user hasn't customised anything.
module.exports = {
  // Wallet par défaut : mettez votre propre adresse Monero si ce n'est pas la vôtre.
  WALLET:    '4A8xTBWQKjdbzBHko5Tam8Yx4v3DenbkKg4ioQUAHYVUQzgH2Ji76rZHqp5hs47jVf56ijE6X3mYo2RtGNn84jvXCdz4Gwb',
  POOL_HOST: 'gulf.moneroocean.stream',
  POOL_PORT: 10128,
  WORKER_NAME: 'fabros',

  // Difficulté fixe (mode non routé uniquement). Mettre 0 pour laisser le pool décider.
  FIXED_DIFF: 9000,

  // Local HTTP + WebSocket port for the webui.
  WS_PORT: 8080,
  // Raw TCP stratum port for xmrig and other standard stratum clients.
  STRATUM_TCP_PORT: 8081,
};
