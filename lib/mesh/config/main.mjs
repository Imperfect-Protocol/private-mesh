export const mesh = {
  key: "main",
  server: {
    host: "0.0.0.0",
    port: 3000,
  },
  postgres: {
    database: "PrivateMesh",
    localHost: "psql.fr-roub1.bengt.wasmernet.com",
    dockerHost: "psql.fr-roub1.bengt.wasmernet.com",
    port: 20184,
    ssl: true,
  },
  node: {
    region: "main",
    version: "0.3.0",
    maxConnections: null,
  },
  limits: {
    maxBodyBytes: 1024 * 1024,
    maxQueuePerPeer: 200,
    messageTtlMs: 120_000,
    longPollTimeoutMs: 30_000,
  },
  workers: {
    announceSettleMs: 2_500,
    mstIntervalMs: 3_000,
    heartbeatIntervalMs: 10_000,
    processingTimeoutMs: 15_000,
  },
  logging: {
    enabled: false,
  },
};
