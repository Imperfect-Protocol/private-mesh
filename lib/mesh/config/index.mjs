import crypto from "node:crypto";
import { existsSync } from "node:fs";
import { mesh as devMesh } from "./dev.mjs";
import { mesh as mainMesh } from "./main.mjs";

const meshes = {
  dev: devMesh,
  main: mainMesh,
};

const selectedMesh = process.env.PRIVATE_MESH_CONFIG ?? "main";
const selectedMeshKey = selectedMesh.toLowerCase();

export const MESH = meshes[selectedMeshKey] ?? mainMesh;
export const MESH_KEY = MESH.key;

export class MeshConfig {
  constructor(env = process.env, mesh = MESH) {
    const postgresUser = env.PRIVATE_MESH_POSTGRES_USER ?? "postgres";
    const postgresPassword = env.PRIVATE_MESH_POSTGRES_PASSWORD ?? "postgres";

    this.key = mesh.key;
    this.server = mesh.server;
    this.postgres = {
      ...mesh.postgres,
      user: postgresUser,
      password: postgresPassword,
      host: postgresHost(mesh.postgres),
    };
    this.databaseUrl = buildPostgresUrl(this.postgres);
    this.nodeId = randomNodeAddress();
    this.publicKey = this.nodeId;
    this.instanceMessage = `${mesh.key}:${process.pid}`;
    this.region = mesh.node.region;
    this.version = mesh.node.version;
    this.maxConnections = mesh.node.maxConnections ?? 0;
    this.maxBodyBytes = mesh.limits.maxBodyBytes;
    this.maxQueuePerPeer = mesh.limits.maxQueuePerPeer;
    this.messageTtlMs = mesh.limits.messageTtlMs;
    this.longPollTimeoutMs = mesh.limits.longPollTimeoutMs;
    this.announceSettleMs = mesh.workers.announceSettleMs;
    this.mstIntervalMs = mesh.workers.mstIntervalMs;
    this.heartbeatIntervalMs = mesh.workers.heartbeatIntervalMs;
    this.processingTimeoutMs = mesh.workers.processingTimeoutMs;
    this.enableLogging = mesh.logging.enabled;
  }

  requireDatabase() {
    if (!this.databaseUrl) {
      throw Object.assign(new Error("Postgres configuration is required for Private Mesh"), {
        statusCode: 503,
      });
    }
  }
}

function buildPostgresUrl(postgres) {
  const user = encodeURIComponent(postgres.user);
  const password = encodeURIComponent(postgres.password);
  const database = encodeURIComponent(postgres.database);
  return `postgres://${user}:${password}@${postgres.host}:${postgres.port}/${database}`;
}

function postgresHost(postgres) {
  if (existsSync("/.dockerenv")) return postgres.dockerHost;
  return postgres.localHost;
}

function randomNodeAddress() {
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}
