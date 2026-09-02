export const mesh = {
  key: "dev",
  server: {
    host: "127.0.0.1",
    port: 3000,
  },
  postgres: {
    database: "postgres",
    localHost: "127.0.0.1",
    dockerHost: "postgres",
    port: 5432,
  },
  node: {
    region: "local",
    version: "0.3.0",
    maxConnections: null,
  },
  limits: {
    maxBodyBytes: 1024 * 1024,
    maxQueuePerPeer: 200,
    messageTtlMs: 120_000,
    longPollTimeoutMs: 1_000,
  },
  workers: {
    announceSettleMs: 2_500,
    mstIntervalMs: 250,
    heartbeatIntervalMs: 10_000,
    processingTimeoutMs: 15_000,
  },
  logging: {
    enabled: true,
  },
};
