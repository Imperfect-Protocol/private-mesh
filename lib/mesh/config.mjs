import crypto from "node:crypto";

export class MeshConfig {
  constructor(env = process.env) {
    this.databaseUrl = env.DATABASE_URL ?? "";
    this.nodeId = normalizeConfiguredNodeId(env.PRIVATE_MESH_NODE_ID);
    this.publicKey = normalizeHex(env.PRIVATE_MESH_PUBLIC_KEY ?? this.nodeId);
    this.instanceMessage = env.PRIVATE_MESH_INSTANCE_MESSAGE ?? `${env.HOSTNAME ?? "local"}:${process.pid}`;
    this.region = env.PRIVATE_MESH_REGION ?? "";
    this.version = env.npm_package_version ?? "0.3.0";
    this.maxConnections = numberEnv(env.PRIVATE_MESH_MAX_CONNECTIONS, 0);
    this.maxBodyBytes = numberEnv(env.PRIVATE_MESH_MAX_BODY_BYTES, 1024 * 1024);
    this.maxQueuePerPeer = numberEnv(env.PRIVATE_MESH_MAX_QUEUE_PER_PEER, 200);
    this.messageTtlMs = numberEnv(env.PRIVATE_MESH_MESSAGE_TTL_MS, 120_000);
    this.longPollTimeoutMs = numberEnv(env.PRIVATE_MESH_LONG_POLL_TIMEOUT_MS, 30_000);
    this.mstIntervalMs = numberEnv(env.PRIVATE_MESH_MST_INTERVAL_MS, numberEnv(env.PRIVATE_MESH_BATCH_INTERVAL_MS, 3_000));
    this.heartbeatIntervalMs = numberEnv(env.PRIVATE_MESH_HEARTBEAT_INTERVAL_MS, 10_000);
    this.processingTimeoutMs = numberEnv(env.PRIVATE_MESH_PROCESSING_TIMEOUT_MS, 15_000);
    this.enableLogging = Boolean(Number(env.PRIVATE_MESH_ENABLE_LOGGING ?? 0));
  }

  requireDatabase() {
    if (!this.databaseUrl) {
      throw Object.assign(new Error("DATABASE_URL is required for Postgres-backed Private Mesh"), {
        statusCode: 503,
      });
    }
  }
}

function numberEnv(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeHex(value) {
  const text = String(value ?? "").trim().toLowerCase();
  return text.startsWith("0x") ? text : `0x${text}`;
}

function normalizeConfiguredNodeId(value) {
  const normalized = normalizeHex(value);
  if (/^0x[0-9a-f]{40}$/.test(normalized)) return normalized;
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}
