import crypto from "node:crypto";
import { normalizeAddress, normalizeHexBytes, shortAddress } from "./mesh/address.mjs";
import { MeshConfig } from "./mesh/config/index.mjs";
import { FlowReactionWorker } from "./mesh/FlowReactionWorker.mjs";
import { FlowState } from "./mesh/flow-states.mjs";
import { HeartbeatWorker } from "./mesh/HeartbeatWorker.mjs";
import { corsHeaders, readJsonBody, sendJson } from "./mesh/http.mjs";
import { LocalOutbox } from "./mesh/LocalOutbox.mjs";
import { LocalPeerRegistry } from "./mesh/LocalPeerRegistry.mjs";
import { MeshLogger } from "./mesh/logger.mjs";
import { MstPlanner } from "./mesh/MstPlanner.mjs";
import { MstWorker } from "./mesh/MstWorker.mjs";
import { NotificationWorker } from "./mesh/NotificationWorker.mjs";
import { PostgresMeshRepository } from "./mesh/PostgresMeshRepository.mjs";

class PostgresSagaMeshService {
  constructor(config = new MeshConfig()) {
    this.config = config;
    this.logger = new MeshLogger(config);
    this.peerRegistry = new LocalPeerRegistry(config);
    this.outbox = new LocalOutbox(config, this.logger);
    this.repository = new PostgresMeshRepository(config, this.logger);
    this.flowWorker = new FlowReactionWorker({
      repository: this.repository,
      peerRegistry: this.peerRegistry,
      outbox: this.outbox,
      logger: this.logger,
    });
    this.mstWorker = new MstWorker({
      repository: this.repository,
      peerRegistry: this.peerRegistry,
      planner: new MstPlanner(),
      config: this.config,
      logger: this.logger,
    });
    this.heartbeatWorker = new HeartbeatWorker({
      repository: this.repository,
      config: this.config,
      logger: this.logger,
    });
    this.notificationWorker = new NotificationWorker({
      repository: this.repository,
      logger: this.logger,
      onWake: (payload) => this.runWorkers(payload),
    });
    this.startPromise = null;
  }

  async start() {
    if (this.startPromise) return this.startPromise;
    this.startPromise = this.startWorkers();
    return this.startPromise;
  }

  async startWorkers() {
    this.repository.start();
    await this.heartbeatWorker.start();
    this.notificationWorker.start();
    this.mstWorker.start();
    await this.runWorkers();
  }

  async stop() {
    this.outbox.closeAll();
    this.mstWorker.stop();
    this.heartbeatWorker.stop();
    await this.notificationWorker.stop();
    await this.repository.stop();
  }

  async runWorkers(notification) {
    if (!this.shouldRunWorkers(notification)) return;
    await this.mstWorker.runOnce();
    await this.flowWorker.runOnce();
  }

  shouldRunWorkers(notification) {
    if (!notification) return true;
    if (notification.type !== "peer-flow-changed") return false;

    const flow = notification.payload ?? {};
    const localPeers = new Set(this.peerRegistry.activeAddresses());
    return localPeers.has(flow.fromPeer) || localPeers.has(flow.toPeer);
  }

  logConfiguration() {
    console.log({
      nodeId: this.config.nodeId,
      databaseUrl: databaseAddress(this.config.databaseUrl),
      maxBodyBytes: this.config.maxBodyBytes,
      maxQueuePerPeer: this.config.maxQueuePerPeer,
      messageTtlMs: this.config.messageTtlMs,
      longPollTimeoutMs: this.config.longPollTimeoutMs,
      announceSettleMs: this.config.announceSettleMs,
      mstIntervalMs: this.config.mstIntervalMs,
      heartbeatIntervalMs: this.config.heartbeatIntervalMs,
      processingTimeoutMs: this.config.processingTimeoutMs,
      enableLogging: this.config.enableLogging,
    });
  }

  async handleRequest(req, res) {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "private.mesh"}`);
    if (url.pathname !== "/mesh" && url.pathname !== "/mesh/grid") return false;

    try {
      if (req.method === "OPTIONS") {
        res.writeHead(204, corsHeaders());
        res.end();
        return true;
      }

      await this.start();

      if (url.pathname === "/mesh/grid" && (req.method === "GET" || req.method === "HEAD")) {
        return this.handleGrid(req, res);
      }

      if (url.pathname === "/mesh" && req.method === "POST") {
        await this.handlePoll(req, res);
        return true;
      }

      if (url.pathname === "/mesh" && (req.method === "GET" || req.method === "HEAD")) {
        return this.handleStatus(req, res);
      }

      sendJson(res, 404, { error: "not found" });
      return true;
    } catch (error) {
      this.logger.error("request.failed", error, { url: req.url });
      if (!res.headersSent) {
        sendJson(res, error?.statusCode ?? 500, {
          error: error?.message ?? "internal server error",
        });
      } else {
        res.end();
      }
      return true;
    }
  }

  handleGrid(req, res) {
    if (req.method === "HEAD") {
      res.writeHead(200, corsHeaders({ "content-type": "application/json; charset=utf-8" }));
      res.end();
      return true;
    }

    const snapshot = this.peerRegistry.snapshot(
      this.outbox.waitingKeys(),
      this.outbox.queuedPeerCount()
    );
    sendJson(res, 200, {
      ...snapshot,
      currentSagaId: "postgres",
      sagaCount: 1,
      openInterests: 0,
      p2pOffloadedCount: 0,
      failedHandshakesCount: 0,
      connectedMstCount: 0,
    });
    return true;
  }

  async handleStatus(req, res) {
    if (req.method === "HEAD") {
      res.writeHead(200, corsHeaders({ "content-type": "application/json; charset=utf-8" }));
      res.end();
      return true;
    }

    const flowCounts = await this.repository.flowCounts();
    sendJson(res, 200, {
      name: "Private Mesh",
      version: 9,
      mode: "postgres-backed-saga",
      nodeId: this.config.nodeId,
      waitingPeerCount: this.outbox.waitingKeys().size,
      queuedPeerCount: this.outbox.queuedPeerCount(),
      localPeerCount: this.peerRegistry.activeAddresses().length,
      flowCounts,
      longPollTimeoutMs: this.config.longPollTimeoutMs,
      announceSettleMs: this.config.announceSettleMs,
      mstIntervalMs: this.config.mstIntervalMs,
    });
    return true;
  }

  async handlePoll(req, res) {
    const body = await readJsonBody(req, this.config.maxBodyBytes);
    const context = normalizePostBody(body);
    this.peerRegistry.remember(context.peer, {
      lobbyId: context.lobbyId,
      tableId: context.tableId,
      kind: "poll",
    });

    for (const message of context.messages) {
      await this.processClientMessage(message, context);
    }

    await this.runWorkers();

    const queued = this.outbox.drain(context.lobbyId, context.tableId, context.peer);
    if (queued.length > 0) {
      sendJson(res, 200, { messages: queued, timeout: false, now: Date.now() });
      return;
    }

    const waiterKey = this.outbox.wait({
      lobbyId: context.lobbyId,
      tableId: context.tableId,
      peer: context.peer,
      res,
      onTimeout: () => this.peerRegistry.remember(context.peer, {
        lobbyId: context.lobbyId,
        tableId: context.tableId,
        kind: "poll.timeout",
      }),
    });

    req.on("close", () => {
      if (this.outbox.releaseWaiter(waiterKey, res)) {
        this.peerRegistry.markDisconnected(context.peer);
      }
    });
  }

  async processClientMessage(message, context) {
    if (!message || typeof message !== "object") return;

    if (message.type === "bootstrap-announce") {
      await this.handleAnnounce(message, context);
      return;
    }

    if (message.type === "bootstrap-interest") {
      const signal = parseSignal(message.signal, "offer");
      const stored = await this.repository.storeOffer(signal);
      if (!stored) {
        this.logger.info("flow.offer.unmatched", {
          flowId: signal.flowId,
          fromPeer: signal.from,
          toPeer: signal.to,
          lobbyId: signal.lobbyId,
          tableId: signal.tableId,
        });
      }
      this.peerRegistry.remember(signal.from, {
        lobbyId: signal.lobbyId,
        tableId: signal.tableId,
        kind: "offer",
      });
      return;
    }

    if (message.type === "bootstrap-answer") {
      const signal = parseSignal(message.signal, "answer");
      const stored = await this.repository.storeAnswer(signal);
      if (!stored) {
        this.logger.info("flow.answer.unmatched", {
          flowId: signal.flowId,
          fromPeer: signal.from,
          toPeer: signal.to,
          lobbyId: signal.lobbyId,
          tableId: signal.tableId,
        });
      }
      this.peerRegistry.remember(signal.from, {
        lobbyId: signal.lobbyId,
        tableId: signal.tableId,
        kind: "answer",
      });
      return;
    }

    if (message.type === "link-connected") {
      await this.repository.markConnected({
        lobbyId: context.lobbyId,
        tableId: context.tableId,
        fromPeer: normalizeAddress(message.from ?? context.peer),
        toPeer: normalizeAddress(message.to ?? message.target),
      });
      return;
    }

    if (message.type === "link-failed") {
      await this.repository.markFailed({
        lobbyId: context.lobbyId,
        tableId: context.tableId,
        fromPeer: normalizeAddress(message.from ?? context.peer),
        toPeer: normalizeAddress(message.to ?? message.target),
        reason: String(message.reason ?? "peer_reported"),
      });
      return;
    }

    if (message.type === "bootstrap-bye") {
      this.peerRegistry.markDisconnected(context.peer);
    }
  }

  async handleAnnounce(message, context) {
    const fromPeer = normalizeAddress(message.peer ?? context.peer);
    const lookingFor = Array.isArray(message.lookingFor) ? message.lookingFor : [];
    this.peerRegistry.remember(fromPeer, {
      lobbyId: context.lobbyId,
      tableId: context.tableId,
      kind: "announce",
    });

    for (const target of lookingFor) {
      const toPeer = normalizeAddress(target);
      if (!toPeer || toPeer === fromPeer) continue;
      await this.repository.insertAnnounce({
        lobbyId: context.lobbyId,
        tableId: context.tableId,
        fromPeer,
        toPeer,
      });
    }
  }
}

function normalizePostBody(value) {
  const peer = normalizeAddress(value?.peer ?? value?.playerAddress ?? value?.originAddress);
  const lobbyId = String(value?.lobbyId ?? "");
  const tableId = String(value?.tableId ?? "");
  if (!peer) throw Object.assign(new Error("peer required"), { statusCode: 400 });
  if (!lobbyId) throw Object.assign(new Error("lobbyId required"), { statusCode: 400 });
  if (!tableId) throw Object.assign(new Error("tableId required"), { statusCode: 400 });

  let messages = [];
  if (Array.isArray(value?.messages)) messages = value.messages;
  if (value?.type) messages = [...messages, value];
  return { peer, lobbyId, tableId, messages };
}

function parseSignal(value, fallbackKind = "offer") {
  const from = normalizeAddress(value?.originAddress ?? value?.sender ?? value?.from);
  const to = normalizeAddress(value?.targetAddress ?? value?.recipient ?? value?.to);
  const encryptedData = normalizeHexBytes(value?.encryptedData ?? value?.payload);
  const kind = String(value?.kind ?? value?.signalType ?? fallbackKind);
  const lobbyId = String(value?.lobbyId ?? "");
  const tableId = String(value?.tableId ?? "");
  const flowId = String(value?.flowId ?? value?.flow_id ?? "");

  if (!from) throw Object.assign(new Error("originAddress required"), { statusCode: 400 });
  if (!to) throw Object.assign(new Error("targetAddress required"), { statusCode: 400 });
  if (!encryptedData) throw Object.assign(new Error("encryptedData required"), { statusCode: 400 });
  if (!lobbyId) throw Object.assign(new Error("lobbyId required"), { statusCode: 400 });
  if (!tableId) throw Object.assign(new Error("tableId required"), { statusCode: 400 });

  return {
    id: crypto.randomUUID(),
    from,
    to,
    encryptedData,
    kind,
    lobbyId,
    tableId,
    flowId,
    sentAt: Date.now(),
  };
}

function databaseAddress(value) {
  if (!value) return "missing";
  try {
    const url = new URL(value);
    return `${url.hostname}:${url.port}${url.pathname}`;
  } catch {
    return "configured";
  }
}

const service = new PostgresSagaMeshService();

export function logConfiguration() {
  service.logConfiguration();
}

export function closeMeshWaiters() {
  void service.stop();
}

export async function handleMeshRequest(req, res) {
  return service.handleRequest(req, res);
}

export { FlowState, shortAddress };
