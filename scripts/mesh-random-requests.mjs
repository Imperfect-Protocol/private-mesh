#!/usr/bin/env node

import crypto from "node:crypto";

const defaults = {
  url: "http://127.0.0.1:3001/mesh",
  graphs: 5,
  throttleStepMs: 500, // 0.5s aligned intervals
  lobbyId: "grid-test",
  failChance: 0.25,     // 25% WebRTC handshake failure chance
  dropPollChance: 0.15, // 15% sudden poll drop chance
};

function argValue(name, fallback) {
  const prefix = `--${name}=`;
  const inline = process.argv.find((arg) => arg.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);

  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1]) return process.argv[index + 1];
  return fallback;
}

function numericArg(name, fallback) {
  const value = Number(argValue(name, fallback));
  return Number.isFinite(value) ? value : fallback;
}

function usage() {
  console.log(`
Usage:
  node scripts/mesh-random-requests.mjs [options]

Options:
  --url <http-url>          Mesh long-poll URL. Default: ${defaults.url}
  --graphs <number>         Number of graph test cases to run. Default: ${defaults.graphs}
  --loop                    Run test cases continuously until stopped.
  --throttle-step-ms <n>    Aligned announcement delay step in ms. Default: ${defaults.throttleStepMs}
  --fail-chance <0..1>      Chance a peer fails WebRTC setup. Default: ${defaults.failChance}
  --drop-poll-chance <0..1> Chance a peer suddenly drops long-poll. Default: ${defaults.dropPollChance}
  --lobby-id <value>        Lobby ID for test runs. Default: ${defaults.lobbyId}

Examples:
  node scripts/mesh-random-requests.mjs --graphs 3
  node scripts/mesh-random-requests.mjs --fail-chance 0.3 --drop-poll-chance 0.2
`);
}

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  usage();
  process.exit(0);
}

function normalizeHttpUrl(value) {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("mesh URL must use http:// or https://");
  }
  return url.toString();
}

const config = {
  url: normalizeHttpUrl(String(argValue("url", defaults.url))),
  graphs: numericArg("graphs", defaults.graphs),
  loop: process.argv.includes("--loop"),
  throttleStepMs: numericArg("throttle-step-ms", defaults.throttleStepMs),
  failChance: Math.min(1, Math.max(0, numericArg("fail-chance", defaults.failChance))),
  dropPollChance: Math.min(1, Math.max(0, numericArg("drop-poll-chance", defaults.dropPollChance))),
  lobbyId: String(argValue("lobby-id", defaults.lobbyId)),
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomAddress() {
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}

function randomBlsHexPayload() {
  return `0x${crypto.randomBytes(96).toString("hex")}`;
}

function shortAddr(addr) {
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

// --- ASCII Graph Visualization ---
function printAsciiGraph(graphIndex, testGraph) {
  console.log("\n" + "=".repeat(72));
  console.log(` TEST CASE #${graphIndex + 1} | Nodes: ${testGraph.nodes.length} | Disjoint Subgraphs: ${testGraph.isDisjoint ? "YES" : "NO"}`);
  console.log("=".repeat(72));

  console.log("\nTopology Diagram:");
  for (const component of testGraph.components) {
    const nodeLabels = component.map((n) => `[${n.label}:${shortAddr(n.address)}]`);
    console.log(`  Component: ${nodeLabels.join(" <---> ")}`);
  }

  console.log("\nDeclared Interest Edges (Looking For):");
  for (const node of testGraph.nodes) {
    const targets = node.lookingFor.map((t) => testGraph.nodes.find((n) => n.address === t)?.label ?? shortAddr(t));
    const targetList = targets.length > 0 ? targets.join(", ") : "none";
    console.log(`  (${node.label}) ${shortAddr(node.address)} ---> [ ${targetList} ]`);
  }

  console.log("\nThrottled Arrival Schedule & Failure Simulation:");
  for (const node of testGraph.nodes) {
    const delaySec = (node.scheduledDelayMs / 1000).toFixed(1);
    const flags = [];
    if (node.willFailWebRtc) flags.push("WILL_FAIL_WEBRTC");
    if (node.willDropPoll) flags.push("WILL_ABRUPT_DROP_POLL");
    const flagStr = flags.length > 0 ? ` [${flags.join(", ")}]` : "";
    console.log(`  T+${delaySec.padStart(4, " ")}s | Node (${node.label}) sends announcement${flagStr}`);
  }
  console.log("-".repeat(72) + "\n");
}

// --- Generator for Graph & Disjoint Subgraphs ---
function generateTestGraph() {
  const totalNodes = crypto.randomInt(3, 9);
  const isDisjoint = Math.random() < 0.5;
  const componentCount = isDisjoint ? crypto.randomInt(2, Math.min(4, totalNodes)) : 1;

  const nodes = [];
  for (let i = 0; i < totalNodes; i++) {
    nodes.push({
      id: i,
      label: String.fromCharCode(65 + i),
      address: randomAddress(),
      lookingFor: [],
      scheduledDelayMs: (i + 1) * config.throttleStepMs,
      willFailWebRtc: Math.random() < config.failChance,
      willDropPoll: Math.random() < config.dropPollChance,
    });
  }

  const components = Array.from({ length: componentCount }, () => []);
  nodes.forEach((node, idx) => {
    components[idx % componentCount].push(node);
  });

  for (const comp of components) {
    if (comp.length === 1) continue;
    for (let i = 0; i < comp.length; i++) {
      const current = comp[i];
      const next = comp[(i + 1) % comp.length];
      current.lookingFor.push(next.address);
    }
  }

  for (const comp of components) {
    if (comp.length > 3 && Math.random() < 0.5) {
      comp[0].lookingFor.push(comp[2].address);
    }
  }

  return { nodes, components, isDisjoint };
}

// --- HTTP Long-Poll Client ---
async function pollMesh(peer, messages, tableId, sagaId, signal) {
  const response = await fetch(config.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      lobbyId: config.lobbyId,
      messages,
      peer,
      saga_id: sagaId,
      tableId,
    }),
    signal,
  });

  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }

  if (!response.ok) {
    throw new Error(body?.error ?? `${response.status} ${response.statusText}`);
  }

  return {
    messages: body?.messages ?? [],
    sagaId: body?.saga_id ?? body?.sagaId ?? sagaId,
  };
}

// --- Simulated Peer Node ---
class SimulatedPeer {
  constructor(node, tableId, callbacks) {
    this.node = node;
    this.tableId = tableId;
    this.address = node.address;
    this.label = node.label;
    this.callbacks = callbacks;
    this.stopped = false;
    this.pendingMessages = [];
    this.controller = null;
    this.sagaId = "";
  }

  start() {
    setTimeout(() => {
      this.pendingMessages.push({
        type: "bootstrap-announce",
        peer: this.address,
        lookingFor: this.node.lookingFor,
      });
      console.log(`[t+${(this.node.scheduledDelayMs / 1000).toFixed(1)}s] Node (${this.label}) sent bootstrap-announce`);

      if (this.node.willDropPoll) {
        setTimeout(() => {
          console.log(`[Node ${this.label}] ABRUPTLY DROPPING LONG-POLL (Simulating Sudden Disconnect)`);
          this.stopAbruptly();
        }, 3_500);
      }

      void this.pollLoop();
    }, this.node.scheduledDelayMs);
  }

  stopAbruptly() {
    this.stopped = true;
    if (this.controller) this.controller.abort();
    this.callbacks.onPeerDropped?.(this.address);
  }

  stop() {
    this.stopped = true;
    if (this.controller) this.controller.abort();
  }

  async pollLoop() {
    while (!this.stopped) {
      const outbound = this.pendingMessages;
      this.pendingMessages = [];
      this.controller = new AbortController();

      try {
        const result = await pollMesh(this.address, outbound, this.tableId, this.sagaId, this.controller.signal);
        this.sagaId = result.sagaId ?? this.sagaId;
        for (const msg of result.messages) {
          await this.handleInboundMessage(msg);
        }
      } catch (err) {
        if (!this.stopped && err.name !== "AbortError") {
          console.error(`[Node ${this.label}] Poll error: ${err.message}`);
          await sleep(500);
        }
      }
    }
  }

  async handleInboundMessage(msg) {
    // 1. Mesh triggers an MST offer creation
    if (msg.type === "trigger-offer" && msg.target) {
      console.log(`[Mesh -> Node ${this.label}] Trigger MST Offer to target ${shortAddr(msg.target)}`);

      if (this.node.willFailWebRtc) {
        console.log(`[Node ${this.label}] WEBRTC FAILURE SIMULATED! Sending link-failed to Mesh for target ${shortAddr(msg.target)}`);
        this.pendingMessages.push({
          type: "link-failed",
          from: this.address,
          to: msg.target,
          reason: "simulated_ice_failure",
        });
        this.callbacks.onHandshakeFailed?.(this.address, msg.target, "simulated_ice_failure");
        return;
      }

      const fakeOfferSignal = {
        originAddress: this.address,
        targetAddress: msg.target,
        lobbyId: config.lobbyId,
        tableId: this.tableId,
        encryptedData: randomBlsHexPayload(),
        kind: "offer",
      };

      this.pendingMessages.push({
        type: "bootstrap-interest",
        signal: fakeOfferSignal,
      });
      return;
    }

    // 2. Mesh asks this peer to complete the reverse P2P edge after the primary edge connected.
    if (msg.type === "p2p-route-available" && msg.status === "connect_through_peers" && msg.target) {
      console.log(`[Mesh -> Node ${this.label}] Connect P2P to target ${shortAddr(msg.target)}`);
      this.pendingMessages.push({
        type: "link-connected",
        from: this.address,
        to: msg.target,
      });
      this.callbacks.onP2pRouteAvailable?.(this.address, msg.target, "ConnectedP2P");
      return;
    }

    // 3. Peer receives an incoming Offer from Mesh -> Responds with Accept
    if (msg.type === "bootstrap-offer" && msg.peer) {
      console.log(`[Node ${this.label}] Received Offer from ${shortAddr(msg.peer)} -> Sending Accept`);

      const fakeAcceptSignal = {
        originAddress: this.address,
        targetAddress: msg.peer,
        lobbyId: config.lobbyId,
        tableId: this.tableId,
        encryptedData: randomBlsHexPayload(),
        kind: "accept",
      };

      this.pendingMessages.push({
        type: "bootstrap-answer",
        signal: fakeAcceptSignal,
      });
      return;
    }

    // 4. Offerer receives Accept answer -> WebRTC connected -> Sends link-connected ack to Mesh
    if (msg.type === "bootstrap-answer" && msg.peer) {
      console.log(`[Node ${this.label}] Handshake Complete with ${shortAddr(msg.peer)}! Sending link-connected ack`);

      this.pendingMessages.push({
        type: "link-connected",
        from: this.address,
        to: msg.peer,
      });

      this.callbacks.onHandshakeComplete?.(this.address, msg.peer);
    }
  }
}

// --- Test Case Runner ---
async function runGraphTestCase(graphIndex) {
  const tableId = `test-table-${Date.now()}-${graphIndex}`;
  const testGraph = generateTestGraph();

  printAsciiGraph(graphIndex, testGraph);

  const completedHandshakes = new Set();
  const failedHandshakes = new Set();
  const p2pRoutesNotified = [];
  const droppedPeers = new Set();

  const callbacks = {
    onHandshakeComplete: (from, to) => completedHandshakes.add(`${from}:${to}`),
    onHandshakeFailed: (from, to, reason) => failedHandshakes.add(`${from}:${to} (${reason})`),
    onP2pRouteAvailable: (from, to, status) => p2pRoutesNotified.push({ from, to, status }),
    onPeerDropped: (addr) => droppedPeers.add(addr),
  };

  const peers = testGraph.nodes.map((n) => new SimulatedPeer(n, tableId, callbacks));

  console.log(`Starting long-poll nodes for Table ${tableId}...`);
  peers.forEach((p) => p.start());

  const maxWaitMs = Math.max(...testGraph.nodes.map((n) => n.scheduledDelayMs)) + 14_000;
  const startWait = Date.now();

  while (Date.now() - startWait < maxWaitMs) {
    await sleep(500);
  }

  console.log(`\nTest Case #${graphIndex + 1} Final Results:`);
  console.log(`  Completed MST Link Handshakes (Server-Orchestrated): ${completedHandshakes.size}`);
  for (const link of completedHandshakes) {
    const [from, to] = link.split(":");
    const fromLabel = testGraph.nodes.find((n) => n.address === from)?.label ?? "?";
    const toLabel = testGraph.nodes.find((n) => n.address === to)?.label ?? "?";
    console.log(`   * (${fromLabel}) ${shortAddr(from)} <==[WEBRTC MST CONNECTED]==> (${toLabel}) ${shortAddr(to)}`);
  }

  console.log(`\n  Failed WebRTC Handshakes (MST Cut / Recomputed): ${failedHandshakes.size}`);
  for (const link of failedHandshakes) {
    const [pair, reason] = link.split(" ");
    const [from, to] = pair.split(":");
    const fromLabel = testGraph.nodes.find((n) => n.address === from)?.label ?? "?";
    const toLabel = testGraph.nodes.find((n) => n.address === to)?.label ?? "?";
    console.log(`   * (${fromLabel}) ${shortAddr(from)} X--[FAILED/CUT: ${reason}]--X (${toLabel}) ${shortAddr(to)}`);
  }

  console.log(`\n  Flushed P2P Offload Messages (Post-MST Execution): ${p2pRoutesNotified.length}`);
  for (const route of p2pRoutesNotified) {
    const fromLabel = testGraph.nodes.find((n) => n.address === route.from)?.label ?? "?";
    const toLabel = testGraph.nodes.find((n) => n.address === route.to)?.label ?? "?";
    console.log(`   * (${fromLabel}) ${shortAddr(route.from)} - - -[P2P GOSSIP: ${route.status.toUpperCase()}]- - -> (${toLabel}) ${shortAddr(route.to)}`);
  }

  if (droppedPeers.size > 0) {
    console.log(`\n  Abrupt Long-Poll Disconnects: ${droppedPeers.size}`);
    for (const addr of droppedPeers) {
      const label = testGraph.nodes.find((n) => n.address === addr)?.label ?? "?";
      console.log(`   * Node (${label}) ${shortAddr(addr)} dropped long-poll prematurely.`);
    }
  }

  peers.forEach((p) => p.stop());
  await sleep(500);
}

// --- Main Loop ---
async function main() {
  console.log("Starting Private Mesh Graph, MST Recovery & Delayed P2P Offload Test Suite...");
  console.log(`Target URL: ${config.url}`);
  console.log(`Throttle Step: ${config.throttleStepMs}ms`);
  console.log(`Simulated Failure Chance: ${config.failChance * 100}%`);
  console.log(`Simulated Poll Drop Chance: ${config.dropPollChance * 100}%`);

  let currentGraph = 0;
  while (config.loop || currentGraph < config.graphs) {
    await runGraphTestCase(currentGraph);
    currentGraph++;
    if (config.loop || currentGraph < config.graphs) {
      console.log("\nPreparing next test case in 2 seconds...");
      await sleep(2000);
    }
  }

  console.log("\nAll graph test cases completed successfully.");
}

main().catch((err) => {
  console.error("Test execution failed:", err);
  process.exit(1);
});
