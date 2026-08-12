import crypto from "node:crypto";

const maxBodyBytes = Number(process.env.PRIVATE_MESH_MAX_BODY_BYTES ?? 1024 * 1024);
const maxQueuePerPeer = Number(process.env.PRIVATE_MESH_MAX_QUEUE_PER_PEER ?? 200);
const messageTtlMs = Number(process.env.PRIVATE_MESH_MESSAGE_TTL_MS ?? 120_000);
const longPollTimeoutMs = Number(process.env.PRIVATE_MESH_LONG_POLL_TIMEOUT_MS ?? 30_000);
const batchIntervalMs = Number(process.env.PRIVATE_MESH_BATCH_INTERVAL_MS ?? 3_000);
const handshakeTimeoutMs = Number(process.env.PRIVATE_MESH_HANDSHAKE_TIMEOUT_MS ?? 6_000);
const enableLogging = Number(process.env.PRIVATE_MESH_ENABLE_LOGGING ?? false);

class Saga {
  constructor(id = crypto.randomUUID()) {
    this.id = id;
    this.createdAt = Date.now();
    this.processingStartedAt = null;
    this.queues = new Map();
    this.waiters = new Map();
    this.interests = new Map();
    this.peerActivity = new Map();
    this.recentPeerEvents = [];

    // Graph & Execution Engine State
    this.tableNodes = new Map();      // tableKey -> Map(peerAddress -> nodeInfo)
    this.tableEdges = new Map();      // tableKey -> Map(edgeKey -> edgeInfo)
    this.tableExecutions = new Map(); // tableKey -> { queue: [], current: null, timer: null }
  }
}

const currentSaga = new Saga("main");

const activityWindowMs = 48 * 60 * 60 * 1000;
const activityBucketMs = 60 * 60 * 1000;

export function logConfiguration() {
  console.log({
    maxBodyBytes,
    maxQueuePerPeer,
    messageTtlMs,
    longPollTimeoutMs,
    batchIntervalMs,
    handshakeTimeoutMs,
    enableLogging,
  });
}

function normalizeHex(value) {
  const text = String(value ?? "").trim();
  return text.startsWith("0x") ? `0x${text.slice(2).toLowerCase()}` : `0x${text.toLowerCase()}`;
}

function normalizeAddress(value) {
  const hex = normalizeHex(value);
  return /^0x[0-9a-f]{40}$/.test(hex) ? hex : "";
}

function normalizeHexBytes(value) {
  const hex = normalizeHex(value);
  return /^0x[0-9a-f]+$/.test(hex) && (hex.length - 2) % 2 === 0 ? hex : "";
}

function shortAddress(value) {
  const text = String(value ?? "");
  return text.length > 12 ? `${text.slice(0, 6)}...${text.slice(-4)}` : text;
}

function tableKey(lobbyId, tableId) {
  return `${String(lobbyId ?? "")}:${String(tableId ?? "")}`;
}

function peerKey(lobbyId, tableId, peer) {
  return `${tableKey(lobbyId, tableId)}:${normalizeAddress(peer)}`;
}

function interestKey(signal) {
  return `${signal.lobbyId}:${signal.tableId}:${signal.from}:${signal.to}`;
}

function summarizePollBody(body) {
  if (!body || typeof body !== "object") {
    return { payload: String(body) };
  }

  const peer = body.peer ? shortAddress(body.peer) : "unknown";
  const messages = Array.isArray(body.messages) 
    ? body.messages 
    : (body.type ? [body] : []);

  const summaryList = messages.map((msg) => {
    switch (msg?.type) {
      case "bootstrap-announce": {
        const targetCount = Array.isArray(msg.lookingFor) ? msg.lookingFor.length : 0;
        return `announce(targets: ${targetCount})`;
      }
      case "bootstrap-interest": {
        const to = shortAddress(msg.signal?.targetAddress ?? msg.signal?.to);
        const kind = msg.signal?.kind ?? "offer";
        return `interest(${kind} -> ${to})`;
      }
      case "bootstrap-answer": {
        const to = shortAddress(msg.signal?.targetAddress ?? msg.signal?.to);
        const kind = msg.signal?.kind ?? "accept";
        return `answer(${kind} -> ${to})`;
      }
      case "link-connected": {
        const target = shortAddress(msg.to ?? msg.target);
        return `link-connected(${target})`;
      }
      case "link-failed": {
        const target = shortAddress(msg.to ?? msg.target);
        const reason = msg.reason ?? "unknown";
        return `link-failed(${target}, reason: ${reason})`;
      }
      case "bootstrap-bye": {
        return "bye";
      }
      default: {
        return msg?.type ?? "raw-payload";
      }
    }
  });

  return {
    peer,
    lobbyId: body.lobbyId ?? "--",
    tableId: body.tableId ?? "--",
    msgCount: messages.length,
    summary: summaryList.length > 0 ? summaryList.join(" | ") : "empty-poll",
  };
}

function allSagas() {
  return [currentSaga];
}

function telemetrySagas() {
  return allSagas();
}

function sumSagaSizes(field) {
  return allSagas().reduce((total, saga) => total + saga[field].size, 0);
}

function pruneSagaInterests(saga, now = Date.now()) {
  const cutoff = now - messageTtlMs;
  for (const [key, interest] of saga.interests.entries()) {
    const lastTouched = interest.answeredAt ?? interest.offeredAt ?? interest.createdAt ?? 0;
    if (lastTouched < cutoff || interest.state === "Answered") {
      saga.interests.delete(key);
    }
  }
}

function isUnresolvedEdge(edge) {
  return edge?.state === "PENDING" || edge?.state === "TRIGGERED";
}

function edgeTouchesPeer(edge, peer) {
  return edge?.from === peer || edge?.to === peer;
}

function peerHasWaiterOrQueue(saga, lobbyId, tableId, peer) {
  const key = peerKey(lobbyId, tableId, peer);
  return saga.waiters.has(key) || saga.queues.has(key);
}

function peerHasUnresolvedWork(saga, lobbyId, tableId, peer) {
  const edges = saga.tableEdges.get(tableKey(lobbyId, tableId));
  if (!edges) return false;
  for (const edge of edges.values()) {
    if (edgeTouchesPeer(edge, peer) && isUnresolvedEdge(edge)) return true;
  }
  return false;
}

function peerKeyHasUnresolvedWork(saga, key) {
  for (const edges of saga.tableEdges.values()) {
    for (const edge of edges.values()) {
      if (
        isUnresolvedEdge(edge) &&
        (peerKey(edge.lobbyId, edge.tableId, edge.from) === key ||
          peerKey(edge.lobbyId, edge.tableId, edge.to) === key)
      ) {
        return true;
      }
    }
  }
  return false;
}

function pruneResolvedInactivePeers(saga, reason = "cleanup", now = Date.now()) {
  for (const [tKey, nodes] of saga.tableNodes.entries()) {
    const edges = saga.tableEdges.get(tKey);
    if (!edges) continue;

    for (const [peer, node] of [...nodes.entries()]) {
      if (peerHasWaiterOrQueue(saga, node.lobbyId, node.tableId, peer)) continue;
      if (peerHasUnresolvedWork(saga, node.lobbyId, node.tableId, peer)) continue;

      let removedEdges = 0;
      for (const [eKey, edge] of [...edges.entries()]) {
        if (edgeTouchesPeer(edge, peer) && !isUnresolvedEdge(edge)) {
          edges.delete(eKey);
          removedEdges += 1;
        }
      }

      nodes.delete(peer);
      logAction("peer.prune", {
        reason,
        peer: shortAddress(peer),
        table: tKey,
        removedEdges,
        idleMs: now - Number(node.lastSeen ?? now),
      }, saga);
    }

    if (edges.size === 0) saga.tableEdges.delete(tKey);
    if (nodes.size === 0) saga.tableNodes.delete(tKey);
    if (!saga.tableNodes.has(tKey)) saga.tableExecutions.delete(tKey);
  }
}

function cleanupSagas(reason = "cleanup") {
  const now = Date.now();
  prunePeerActivity(currentSaga, now);
  pruneSagaInterests(currentSaga, now);
  pruneResolvedInactivePeers(currentSaga, reason, now);
}

function logAction(action, fields = {}, saga = currentSaga) {
  if (!enableLogging) return;
  const timestamp = new Date().toISOString();
  console.log(timestamp, "[mesh]", action, {
    sagaId: saga?.id,
    queues: saga?.queues.size ?? 0,
    waiters: saga?.waiters.size ?? 0,
    interests: saga?.interests.size ?? 0,
    ...fields,
  });
}

function parseSignal(saga, value, fallbackKind = "offer") {
  const from = normalizeAddress(value?.originAddress ?? value?.sender ?? value?.from);
  const to = normalizeAddress(value?.targetAddress ?? value?.recipient ?? value?.to);
  const encryptedData = normalizeHexBytes(value?.encryptedData ?? value?.payload);
  const kind = String(value?.kind ?? value?.signalType ?? fallbackKind);
  const lobbyId = String(value?.lobbyId ?? "");
  const tableId = String(value?.tableId ?? "");

  if (!from) throw Object.assign(new Error("originAddress required"), { statusCode: 400 });
  if (!to) throw Object.assign(new Error("targetAddress required"), { statusCode: 400 });
  if (!encryptedData) throw Object.assign(new Error("encryptedData required"), { statusCode: 400 });
  if (!lobbyId) throw Object.assign(new Error("lobbyId required"), { statusCode: 400 });
  if (!tableId) throw Object.assign(new Error("tableId required"), { statusCode: 400 });

  const signal = {
    id: crypto.randomUUID(),
    from,
    to,
    encryptedData,
    kind,
    lobbyId,
    tableId,
    sentAt: Date.now(),
  };
  rememberPeer(saga, from, { kind, lobbyId, tableId });
  rememberPeer(saga, to, { kind: `${kind}.target`, lobbyId, tableId });
  return signal;
}

function envelope(signal, recipient) {
  return {
    id: signal.id,
    sent_at: signal.sentAt,
    sender: signal.from,
    originAddress: signal.from,
    recipient,
    payload: signal.encryptedData,
    encryptedData: signal.encryptedData,
    kind: signal.kind,
    lobbyId: signal.lobbyId,
    tableId: signal.tableId,
  };
}

function prunePeerActivity(saga, now = Date.now()) {
  const cutoff = now - activityWindowMs;
  while (saga.recentPeerEvents.length > 0 && saga.recentPeerEvents[0].at < cutoff) {
    saga.recentPeerEvents.shift();
  }
  for (const [address, peer] of saga.peerActivity.entries()) {
    if (peer.lastSeenAt < cutoff) saga.peerActivity.delete(address);
  }
}

function rememberPeer(saga, peer, fields = {}) {
  const address = normalizeAddress(peer);
  if (!address) return;

  const now = Date.now();
  const current = saga.peerActivity.get(address) ?? {
    address,
    firstSeenAt: now,
    lastSeenAt: now,
    count: 0,
    lobbyId: "",
    tableId: "",
    status: "ACTIVE",
  };

  current.lastSeenAt = now;
  current.count += 1;
  current.status = "ACTIVE";
  current.lobbyId = fields.lobbyId ?? current.lobbyId;
  current.tableId = fields.tableId ?? current.tableId;
  saga.peerActivity.set(address, current);

  saga.recentPeerEvents.push({
    address,
    at: now,
    kind: fields.kind ?? "activity",
    lobbyId: fields.lobbyId ?? "",
    tableId: fields.tableId ?? "",
    sagaId: saga.id,
  });
  prunePeerActivity(saga, now);
}

function corsHeaders(extra = {}) {
  return {
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET, HEAD, POST, OPTIONS",
    "access-control-allow-origin": "*",
    "cache-control": "no-store",
    ...extra,
  };
}

function sendJson(res, statusCode, value) {
  const body = JSON.stringify(value);
  res.writeHead(statusCode, corsHeaders({
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  }));
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > maxBodyBytes) {
        reject(Object.assign(new Error("request body too large"), { statusCode: 413 }));
        req.destroy();
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function pruneQueue(saga, key, now = Date.now()) {
  const messages = saga.queues.get(key);
  if (!messages) return [];
  const fresh = messages
    .filter((message) => Number(message.expiresAt ?? 0) > now)
    .slice(-maxQueuePerPeer);
  if (fresh.length > 0) {
    saga.queues.set(key, fresh);
  } else {
    saga.queues.delete(key);
  }
  return fresh;
}

function drainQueue(saga, key) {
  const messages = pruneQueue(saga, key);
  if (messages.length === 0) return [];
  saga.queues.delete(key);
  return messages.map(({ expiresAt: _expiresAt, ...message }) => message);
}

function finishWaiter(saga, key, messages = [], timeout = false) {
  const waiter = saga.waiters.get(key);
  if (!waiter) return false;
  saga.waiters.delete(key);
  clearTimeout(waiter.timer);
  const keepAlive = timeout && messages.length === 0 && peerKeyHasUnresolvedWork(saga, key);
  const responseMessages = keepAlive
    ? [{ type: "keep-alive", saga_id: saga.id, sagaId: saga.id }]
    : messages;
  sendJson(waiter.res, 200, {
    saga_id: saga.id,
    sagaId: saga.id,
    messages: responseMessages,
    timeout: timeout && !keepAlive,
    now: Date.now(),
  });
  cleanupSagas(keepAlive ? "waiter-keep-alive" : (timeout ? "waiter-timeout" : "waiter-finish"));
  return true;
}

function enqueueMessage(saga, { lobbyId, tableId, recipient, message }, keysToFlush = null) {
  const key = peerKey(lobbyId, tableId, recipient);
  const queuedMessage = {
    ...message,
    saga_id: saga.id,
    sagaId: saga.id,
    id: message.id ?? crypto.randomUUID(),
    queuedAt: Date.now(),
    expiresAt: Date.now() + messageTtlMs,
  };

  const messages = pruneQueue(saga, key);
  messages.push(queuedMessage);
  saga.queues.set(key, messages.slice(-maxQueuePerPeer));

  if (keysToFlush) {
    keysToFlush.add(key);
  } else if (saga.waiters.has(key)) {
    const drained = drainQueue(saga, key);
    if (drained.length > 0) finishWaiter(saga, key, drained, false);
  }

  logAction("message.enqueue", {
    key,
    type: message.type,
    recipient: shortAddress(recipient),
  }, saga);
}

// Store Announcements and Edges
function rememberAnnounce(saga, lobbyId, tableId, peer, lookingFor = []) {
  const tKey = tableKey(lobbyId, tableId);
  const now = Date.now();

  if (!saga.tableNodes.has(tKey)) saga.tableNodes.set(tKey, new Map());
  if (!saga.tableEdges.has(tKey)) saga.tableEdges.set(tKey, new Map());

  const nodes = saga.tableNodes.get(tKey);
  const edges = saga.tableEdges.get(tKey);

  nodes.set(peer, {
    peer,
    lobbyId,
    tableId,
    lastSeen: now,
    status: "ACTIVE",
  });

  for (const dest of lookingFor) {
    const target = normalizeAddress(dest);
    if (!target || target === peer) continue;

    const eKey = `${peer}:${target}`;
    const previous = edges.get(eKey);
    edges.set(eKey, {
      from: peer,
      to: target,
      lobbyId,
      tableId,
      timestamp: now,
      state: "PENDING",
    });

    logAction(previous ? "graph.edge-replace" : "graph.edge-add", {
      from: shortAddress(peer),
      to: shortAddress(target),
      previousState: previous?.state,
      table: tKey,
    }, saga);
  }
}

// Compute Prim's MST over Disjoint Subgraphs
function computePrimMST(activeNodes, activeEdges) {
  const adj = new Map();
  for (const node of activeNodes) adj.set(node, []);

  for (const edge of activeEdges) {
    if (adj.has(edge.from) && adj.has(edge.to)) {
      adj.get(edge.from).push(edge);
    }
  }

  const visited = new Set();
  const mst = [];

  for (const startNode of activeNodes) {
    if (visited.has(startNode)) continue;

    visited.add(startNode);
    const minHeap = [];

    const addEdges = (node) => {
      const outgoing = adj.get(node) ?? [];
      for (const edge of outgoing) {
        if (!visited.has(edge.to)) {
          minHeap.push(edge);
        }
      }
      minHeap.sort((a, b) => a.timestamp - b.timestamp);
    };

    addEdges(startNode);

    while (minHeap.length > 0) {
      const minEdge = minHeap.shift();
      if (visited.has(minEdge.to)) continue;

      visited.add(minEdge.to);
      mst.push(minEdge);
      addEdges(minEdge.to);
    }
  }

  return mst;
}

// Graph Reachability Helper (BFS through CONNECTED edges)
function isConnectedViaMst(saga, tKey, from, to) {
  const edges = saga.tableEdges.get(tKey);
  if (!edges) return false;

  const adj = new Map();
  for (const edge of edges.values()) {
    if (edge.state === "CONNECTED") {
      if (!adj.has(edge.from)) adj.set(edge.from, []);
      if (!adj.has(edge.to)) adj.set(edge.to, []);
      adj.get(edge.from).push(edge.to);
      adj.get(edge.to).push(edge.from);
    }
  }

  if (!adj.has(from) || !adj.has(to)) return false;

  const visited = new Set([from]);
  const queue = [from];

  while (queue.length > 0) {
    const curr = queue.shift();
    if (curr === to) return true;
    for (const neighbor of (adj.get(curr) ?? [])) {
      if (!visited.has(neighbor)) {
        visited.add(neighbor);
        queue.push(neighbor);
      }
    }
  }
  return false;
}

// Flush non-MST edges AFTER all MST attempts complete
function flushP2pOffloads(saga, tKey, keysToFlush = null) {
  const edges = saga.tableEdges.get(tKey);
  if (!edges) return;

  for (const edge of edges.values()) {
    if (edge.state === "PENDING") {
      edge.state = "OFFLOADED_P2P";
      const reachable = isConnectedViaMst(saga, tKey, edge.from, edge.to);
      const status = reachable ? "connect_through_peers" : "cut";

      enqueueMessage(saga, {
        lobbyId: edge.lobbyId,
        tableId: edge.tableId,
        recipient: edge.from,
        message: {
          type: "p2p-route-available",
          target: edge.to,
          status,
          lobbyId: edge.lobbyId,
          tableId: edge.tableId,
        },
      }, keysToFlush);

      logAction("p2p.route_notify", {
        from: shortAddress(edge.from),
        to: shortAddress(edge.to),
        status,
      }, saga);
    }
  }
}

// Recompute MST after a Handshake Failure
function recomputeAndResumeMst(saga, tKey, keysToFlush = null) {
  const nodes = saga.tableNodes.get(tKey);
  const edges = saga.tableEdges.get(tKey);
  if (!nodes || !edges) return;

  const now = Date.now();
  const activeCutoff = now - longPollTimeoutMs;
  const activeNodeSet = new Set();

  for (const [peer, info] of nodes.entries()) {
    if (info.lastSeen >= activeCutoff && info.status !== "DEAD") {
      activeNodeSet.add(peer);
    }
  }

  const activePendingEdges = [];
  for (const edge of edges.values()) {
    if (edge.state === "PENDING" && activeNodeSet.has(edge.from) && activeNodeSet.has(edge.to)) {
      activePendingEdges.push(edge);
    }
  }

  const mst = computePrimMST(activeNodeSet, activePendingEdges);
  const execution = saga.tableExecutions.get(tKey) ?? { queue: [], current: null, timer: null };
  saga.tableExecutions.set(tKey, execution);

  execution.queue = mst;
  processTableQueue(saga, tKey, keysToFlush);
}

// Execution Engine for Handshake Triggers
function processTableQueue(saga, tKey, keysToFlush = null) {
  let execution = saga.tableExecutions.get(tKey);
  if (!execution) {
    execution = { queue: [], current: null, timer: null };
    saga.tableExecutions.set(tKey, execution);
  }

  if (execution.current) return;

  if (execution.queue.length === 0) {
    // MST edges completely processed -> Flush P2P Offloads
    flushP2pOffloads(saga, tKey, keysToFlush);
    cleanupSagas("queue-empty");
    return;
  }

  const edge = execution.queue.shift();
  execution.current = edge;
  edge.state = "TRIGGERED";

  enqueueMessage(saga, {
    lobbyId: edge.lobbyId,
    tableId: edge.tableId,
    recipient: edge.from,
    message: {
      type: "trigger-offer",
      target: edge.to,
      lobbyId: edge.lobbyId,
      tableId: edge.tableId,
    },
  }, keysToFlush);

  logAction("handshake.trigger", {
    from: shortAddress(edge.from),
    to: shortAddress(edge.to),
  }, saga);

  execution.timer = setTimeout(() => {
    logAction("handshake.timeout", {
      from: shortAddress(edge.from),
      to: shortAddress(edge.to),
    }, saga);
    handleHandshakeFailure(saga, edge.lobbyId, edge.tableId, edge.from, edge.to, "timeout");
  }, handshakeTimeoutMs);
}

function markHandshakeComplete(saga, lobbyId, tableId, from, to, keysToFlush = null) {
  const tKey = tableKey(lobbyId, tableId);
  const execution = saga.tableExecutions.get(tKey);
  const edges = saga.tableEdges.get(tKey);
  const matchesCurrent = Boolean(
    execution &&
    execution.current &&
    execution.current.from === from &&
    execution.current.to === to
  );

  if (edges) {
    const eKey = `${from}:${to}`;
    const edge = edges.get(eKey);
    if (edge && matchesCurrent) {
      edge.state = "CONNECTED";
    } else if (edge) {
      logAction("handshake.complete-stale", {
        from: shortAddress(from),
        to: shortAddress(to),
        state: edge.state,
      }, saga);
      return;
    }
  }

  if (matchesCurrent) {
    clearTimeout(execution.timer);
    execution.current = null;
    logAction("handshake.complete", {
      from: shortAddress(from),
      to: shortAddress(to),
    }, saga);
    processTableQueue(saga, tKey, keysToFlush);
    cleanupSagas("handshake-complete");
  }
}

function handleHandshakeFailure(saga, lobbyId, tableId, from, to, reason = "failed", keysToFlush = null) {
  const tKey = tableKey(lobbyId, tableId);
  const execution = saga.tableExecutions.get(tKey);
  const edges = saga.tableEdges.get(tKey);
  const matchesCurrent = Boolean(
    execution &&
    execution.current &&
    execution.current.from === from &&
    execution.current.to === to
  );

  if (edges) {
    const eKey = `${from}:${to}`;
    const edge = edges.get(eKey);
    if (edge && matchesCurrent) {
      edge.state = "FAILED";
    } else if (edge) {
      logAction("handshake.failed-stale", {
        from: shortAddress(from),
        to: shortAddress(to),
        state: edge.state,
        reason,
      }, saga);
      return;
    }
  }

  if (matchesCurrent) {
    clearTimeout(execution.timer);
    execution.current = null;
    logAction("handshake.failed", {
      from: shortAddress(from),
      to: shortAddress(to),
      reason,
    }, saga);
    recomputeAndResumeMst(saga, tKey, keysToFlush);
    cleanupSagas("handshake-failed");
  }
}

// Batch Loop (Prim's MST Construction over the rolling live graph)
setInterval(() => {
  cleanupSagas("batch-start");
  const saga = currentSaga;
  if (saga.tableNodes.size === 0) return;

  saga.processingStartedAt = Date.now();
  logAction("batch.process", {
    tables: saga.tableNodes.size,
  }, saga);

  const now = Date.now();
  const activeCutoff = now - longPollTimeoutMs;

  for (const [tKey, nodes] of saga.tableNodes.entries()) {
    const activeNodeSet = new Set();
    for (const [peer, info] of nodes.entries()) {
      if (info.lastSeen >= activeCutoff && info.status !== "DEAD") {
        activeNodeSet.add(peer);
      }
    }

    const edges = saga.tableEdges.get(tKey);
    if (!edges || activeNodeSet.size < 2) continue;

    const activePendingEdges = [];
    for (const edge of edges.values()) {
      if (
        edge.state === "PENDING" &&
        activeNodeSet.has(edge.from) &&
        activeNodeSet.has(edge.to)
      ) {
        activePendingEdges.push(edge);
      }
    }

    if (activePendingEdges.length === 0) continue;

    const mst = computePrimMST(activeNodeSet, activePendingEdges);
    if (mst.length === 0) continue;

    let execution = saga.tableExecutions.get(tKey);
    if (!execution) {
      execution = { queue: [], current: null, timer: null };
      saga.tableExecutions.set(tKey, execution);
    }

    for (const edge of mst) {
      if (!execution.queue.some((e) => e.from === edge.from && e.to === edge.to)) {
        execution.queue.push(edge);
      }
    }

    processTableQueue(saga, tKey);
  }
  cleanupSagas("batch-end");
}, batchIntervalMs);

function storeInterest(saga, signal, keysToFlush = null) {
  const key = interestKey(signal);
  const interest = {
    key,
    state: "Offered",
    offer: signal,
    answer: null,
    createdAt: Date.now(),
    offeredAt: Date.now(),
    answeredAt: null,
  };
  saga.interests.set(key, interest);

  enqueueMessage(saga, {
    lobbyId: signal.lobbyId,
    tableId: signal.tableId,
    recipient: signal.to,
    message: {
      type: "bootstrap-offer",
      interestId: key,
      peer: signal.from,
      signal: envelope(signal, signal.to),
    },
  }, keysToFlush);

  logAction("interest.store", { signal: signalLog(signal) }, saga);
}

function storeAnswer(saga, signal, keysToFlush = null) {
  const key = `${signal.lobbyId}:${signal.tableId}:${signal.to}:${signal.from}`;
  const interest = saga.interests.get(key);

  enqueueMessage(saga, {
    lobbyId: signal.lobbyId,
    tableId: signal.tableId,
    recipient: signal.to,
    message: {
      type: "bootstrap-answer",
      interestId: interest?.key ?? key,
      peer: signal.from,
      signal: envelope(signal, signal.to),
    },
  }, keysToFlush);

  if (interest) {
    interest.answer = signal;
    interest.state = "Answered";
    interest.answeredAt = Date.now();
  }

  logAction("answer.forward", { signal: signalLog(signal), key }, saga);
}

function signalLog(signal) {
  return {
    id: signal?.id,
    from: shortAddress(signal?.from),
    to: shortAddress(signal?.to),
    lobbyId: signal?.lobbyId,
    tableId: signal?.tableId,
    kind: signal?.kind,
    payloadBytes: Math.max(0, (String(signal?.encryptedData ?? "").length - 2) / 2),
  };
}

function processClientMessage(saga, message, context, keysToFlush = null) {
  if (!message || typeof message !== "object") return;

  if (message.type === "bootstrap-announce") {
    const peer = normalizeAddress(message.peer ?? context.peer);
    const lookingFor = Array.isArray(message.lookingFor) ? message.lookingFor : [];
    rememberAnnounce(saga, context.lobbyId, context.tableId, peer, lookingFor);
    return;
  }

  if (message.type === "bootstrap-interest") {
    const signal = parseSignal(saga, message.signal, "offer");
    storeInterest(saga, signal, keysToFlush);
    return;
  }

  if (message.type === "bootstrap-answer") {
    const signal = parseSignal(saga, message.signal, "answer");
    storeAnswer(saga, signal, keysToFlush);
    return;
  }

  if (message.type === "link-connected") {
    const from = normalizeAddress(message.from ?? context.peer);
    const to = normalizeAddress(message.to ?? message.target);
    markHandshakeComplete(saga, context.lobbyId, context.tableId, from, to, keysToFlush);
    return;
  }

  if (message.type === "link-failed") {
    const from = normalizeAddress(message.from ?? context.peer);
    const to = normalizeAddress(message.to ?? message.target);
    handleHandshakeFailure(saga, context.lobbyId, context.tableId, from, to, message.reason ?? "peer_reported", keysToFlush);
    return;
  }

  if (message.type === "bootstrap-bye") {
    const peer = normalizeAddress(context.peer);
    const peerInfo = saga.peerActivity.get(peer);
    if (peerInfo) peerInfo.status = "DISCONNECTED";

    logAction("peer.bye", {
      peer: shortAddress(context.peer),
      lobbyId: context.lobbyId,
      tableId: context.tableId,
    }, saga);
  }
}

function normalizePostBody(value) {
  const peer = normalizeAddress(value?.peer ?? value?.playerAddress ?? value?.originAddress);
  const lobbyId = String(value?.lobbyId ?? "");
  const tableId = String(value?.tableId ?? "");
  const sagaId = String(value?.saga_id ?? value?.sagaId ?? "");
  if (!peer) throw Object.assign(new Error("peer required"), { statusCode: 400 });
  if (!lobbyId) throw Object.assign(new Error("lobbyId required"), { statusCode: 400 });
  if (!tableId) throw Object.assign(new Error("tableId required"), { statusCode: 400 });

  let messages = [];
  if (Array.isArray(value?.messages)) messages = value.messages;
  if (value?.type) messages = [...messages, value];

  return { peer, lobbyId, tableId, sagaId, messages };
}

async function handleMeshPoll(req, res) {
  const raw = await readBody(req);
  const body = raw ? JSON.parse(raw) : {};
  const context = normalizePostBody(body);
  const saga = currentSaga;
  const key = peerKey(context.lobbyId, context.tableId, context.peer);
  logAction("poll.received", summarizePollBody(body), saga);

  rememberPeer(saga, context.peer, {
    kind: "poll",
    lobbyId: context.lobbyId,
    tableId: context.tableId,
  });

  rememberAnnounce(saga, context.lobbyId, context.tableId, context.peer, []);

  // Define keysToFlush set before the loop
  const keysToFlush = new Set();

  // 1. Process all incoming messages from client request first
  for (const message of context.messages) {
    processClientMessage(saga, message, context, keysToFlush);
  }

  // Flush all accumulated queues after the loop
  for (const flushKey of keysToFlush) {
    if (saga.waiters.has(flushKey)) {
      const drained = drainQueue(saga, flushKey);
      if (drained.length > 0) {
        finishWaiter(saga, flushKey, drained, false);
      }
    }
  }

  // 2. Check if messages were queued for this peer
  const queued = drainQueue(saga, key);
  if (queued.length > 0) {
    sendJson(res, 200, {
      saga_id: saga.id,
      sagaId: saga.id,
      messages: queued,
      timeout: false,
      now: Date.now(),
    });
    return;
  }

  // 3. Queue is empty -> DO NOT send immediate response.
  // Park request in waiters map for long-polling.
  const existing = saga.waiters.get(key);
  if (existing) {
    finishWaiter(saga, key, [], true);
  }

  const timer = setTimeout(() => {
    finishWaiter(saga, key, [], true);
  }, longPollTimeoutMs);

  saga.waiters.set(key, { res, timer });

  // Sudden Long-Poll Drop Handling
  req.on("close", () => {
    const waiter = saga.waiters.get(key);
    if (waiter?.res !== res) return;
    saga.waiters.delete(key);
    clearTimeout(timer);

    const peerInfo = saga.peerActivity.get(context.peer);
    if (peerInfo) {
      peerInfo.status = "DISCONNECTED";
    }

    logAction("poll.aborted", {
      peer: shortAddress(context.peer),
      lobbyId: context.lobbyId,
      tableId: context.tableId,
    }, saga);
  });
}

function statusBody() {
  return {
    name: "Private Mesh",
    version: 8,
    mode: "http-long-poll-rolling-mst",
    currentSagaId: currentSaga.id,
    sagaCount: 1,
    longPollTimeoutMs,
    batchIntervalMs,
    handshakeTimeoutMs,
    queuedPeerCount: sumSagaSizes("queues"),
    waitingPeerCount: sumSagaSizes("waiters"),
    openInterests: sumSagaSizes("interests"),
  };
}

function meshGridSnapshot() {
  const now = Date.now();
  for (const saga of telemetrySagas()) prunePeerActivity(saga, now);

  const bucketStart = Math.floor(now / activityBucketMs) * activityBucketMs - 47 * activityBucketMs;
  const buckets = Array.from({ length: 48 }, (_, index) => ({
    startedAt: bucketStart + index * activityBucketMs,
    count: 0,
  }));

  let eventCount48h = 0;
  for (const saga of telemetrySagas()) {
    eventCount48h += saga.recentPeerEvents.length;
    for (const event of saga.recentPeerEvents) {
      const index = Math.floor((event.at - bucketStart) / activityBucketMs);
      if (index >= 0 && index < buckets.length) buckets[index].count += 1;
    }
  }

  let p2pOffloadedCount = 0;
  let failedHandshakesCount = 0;
  let connectedMstCount = 0;

  for (const saga of allSagas()) {
    for (const edges of saga.tableEdges.values()) {
      for (const edge of edges.values()) {
        if (edge.state === "OFFLOADED_P2P") p2pOffloadedCount += 1;
        if (edge.state === "FAILED") failedHandshakesCount += 1;
        if (edge.state === "CONNECTED") connectedMstCount += 1;
      }
    }
  }

  const waitingKeys = new Set();
  const latestPeers = new Map();
  for (const saga of allSagas()) {
    for (const key of saga.waiters.keys()) waitingKeys.add(key);
  }
  for (const saga of telemetrySagas()) {
    for (const peer of saga.peerActivity.values()) {
      const current = latestPeers.get(peer.address);
      if (!current || peer.lastSeenAt > current.lastSeenAt) {
        latestPeers.set(peer.address, peer);
      }
    }
  }

  const peers = [...latestPeers.values()]
    .sort((a, b) => b.lastSeenAt - a.lastSeenAt)
    .map((peer) => ({
      address: peer.address,
      display: shortAddress(peer.address),
      count: peer.count,
      active: [...waitingKeys].some((key) => key.endsWith(`:${peer.address}`)),
      status: peer.status ?? (now - peer.lastSeenAt < longPollTimeoutMs ? "ACTIVE" : "IDLE"),
      firstSeenAt: peer.firstSeenAt,
      lastSeenAt: peer.lastSeenAt,
      lobbyId: peer.lobbyId,
      tableId: peer.tableId,
    }));

  return {
    generatedAt: now,
    windowHours: 48,
    currentSagaId: currentSaga.id,
    sagaCount: 1,
    activeCount: sumSagaSizes("waiters"),
    recentPeerCount: peers.length,
    eventCount48h,
    queuedPeerCount: sumSagaSizes("queues"),
    openInterests: sumSagaSizes("interests"),
    p2pOffloadedCount,
    failedHandshakesCount,
    connectedMstCount,
    peers,
    buckets,
  };
}

export async function handleMeshRequest(req, res) {
  logAction(`handleMeshRequest: ${req.url}`);
  const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "private.mesh"}`);
  if (url.pathname !== "/mesh" && url.pathname !== "/mesh/grid") {
    return false;
  }

  try {
    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      res.end();
      return true;
    }

    if (url.pathname === "/mesh/grid" && (req.method === "GET" || req.method === "HEAD")) {
      if (req.method === "HEAD") {
        res.writeHead(200, corsHeaders({ "content-type": "application/json; charset=utf-8" }));
        res.end();
      } else {
        sendJson(res, 200, meshGridSnapshot());
      }
      return true;
    }

    if (url.pathname === "/mesh" && req.method === "POST") {
      await handleMeshPoll(req, res);
      return true;
    }

    if (url.pathname === "/mesh" && (req.method === "GET" || req.method === "HEAD")) {
      if (req.method === "HEAD") {
        res.writeHead(200, corsHeaders({ "content-type": "application/json; charset=utf-8" }));
        res.end();
      } else {
        sendJson(res, 200, statusBody());
      }
      return true;
    }

    sendJson(res, 404, { error: "not found" });
    return true;
  } catch (error) {
    console.error("[mesh] error", error);
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

export function closeMeshWaiters() {
  for (const saga of allSagas()) {
    for (const key of [...saga.waiters.keys()]) finishWaiter(saga, key, [], true);
  }
}
