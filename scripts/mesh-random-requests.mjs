#!/usr/bin/env node

import crypto from "node:crypto";

const defaults = {
  count: 24,
  intervalMs: 80,
  keepMs: 45_000,
  lobbyId: "grid",
  maxOpenPerSecond: 5,
  maxSessions: 80,
  minOpenPerSecond: 1,
  randomCloseChance: 0.24,
  randomCloseEveryMs: 1000,
  tableId: String(Date.now()),
  url: "http://127.0.0.1:3001/mesh",
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
  return Number.isFinite(value) && value >= 0 ? value : fallback;
}

function usage() {
  console.log(`
Usage:
  node scripts/mesh-random-requests.mjs [options]

Options:
  --url <http-url>        Mesh long-poll URL. Default: ${defaults.url}
  --count <number>        Number of random connection requests. Default: ${defaults.count}
  --interval-ms <number>  Delay between starting sessions. Default: ${defaults.intervalMs}
  --keep-ms <number>      How long to keep sessions polling. Default: ${defaults.keepMs}
  --loop                  Keep opening and closing random sessions until stopped.
  --min-open-rate <n>     Loop mode: minimum new sessions per second. Default: ${defaults.minOpenPerSecond}
  --max-open-rate <n>     Loop mode: maximum new sessions per second. Default: ${defaults.maxOpenPerSecond}
  --max-sessions <number> Loop mode: maximum sessions kept open. Default: ${defaults.maxSessions}
  --close-chance <0..1>   Loop mode: chance to close each open session per tick. Default: ${defaults.randomCloseChance}
  --close-every-ms <n>    Loop mode: close tick interval. Default: ${defaults.randomCloseEveryMs}
  --lobby-id <value>      Lobby id attached to generated requests. Default: ${defaults.lobbyId}
  --table-id <value>      Table id attached to generated requests. Default: current timestamp

Examples:
  node scripts/mesh-random-requests.mjs --url http://127.0.0.1:3001/mesh --count 80 --keep-ms 60000
  node scripts/mesh-random-requests.mjs --loop --url https://private-mesh.wasmer.app/mesh --min-open-rate 1 --max-open-rate 5
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
  closeEveryMs: numericArg("close-every-ms", defaults.randomCloseEveryMs),
  count: numericArg("count", defaults.count),
  intervalMs: numericArg("interval-ms", defaults.intervalMs),
  keepMs: numericArg("keep-ms", defaults.keepMs),
  lobbyId: String(argValue("lobby-id", defaults.lobbyId)),
  loop: process.argv.includes("--loop"),
  maxOpenPerSecond: numericArg("max-open-rate", defaults.maxOpenPerSecond),
  maxSessions: numericArg("max-sessions", defaults.maxSessions),
  minOpenPerSecond: numericArg("min-open-rate", defaults.minOpenPerSecond),
  randomCloseChance: Math.min(1, numericArg("close-chance", defaults.randomCloseChance)),
  tableId: String(argValue("table-id", defaults.tableId)),
  url: normalizeHttpUrl(String(argValue("url", defaults.url))),
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomAddress() {
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}

function randomInt(min, max) {
  const lower = Math.ceil(Math.min(min, max));
  const upper = Math.floor(Math.max(min, max));
  return lower + crypto.randomInt(Math.max(1, upper - lower + 1));
}

function randomPayload(bytes = 192) {
  return `0x${crypto.randomBytes(bytes).toString("hex")}`;
}

function makeSignal(from, to, kind = "offer") {
  return {
    encryptedData: randomPayload(),
    kind,
    lobbyId: config.lobbyId,
    originAddress: from,
    tableId: config.tableId,
    targetAddress: to,
  };
}

async function poll(peer, messages = []) {
  const response = await fetch(config.url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      lobbyId: config.lobbyId,
      messages,
      peer,
      tableId: config.tableId,
    }),
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

  return body?.messages ?? [];
}

class MeshSession {
  constructor(from, to, index) {
    this.from = from;
    this.to = to;
    this.index = index;
    this.closed = false;
    this.pending = [{
      signal: makeSignal(from, to, "offer"),
      type: "bootstrap-interest",
    }];
    this.label = `${String(index + 1).padStart(3, "0")} ${from.slice(0, 10)} -> ${to.slice(0, 10)}`;
  }

  close() {
    this.closed = true;
  }

  async runUntil(deadline) {
    console.log(`[mesh-random] open ${this.label}`);
    while (!this.closed && Date.now() < deadline) {
      const outbound = this.pending;
      this.pending = [];
      let inbound = [];
      try {
        inbound = await poll(this.from, outbound);
      } catch (error) {
        console.error(`[mesh-random] error ${this.label}: ${error.message}`);
        await sleep(250);
        continue;
      }

      for (const message of inbound) {
        this.handleMessage(message);
      }
    }

    try {
      await poll(this.from, [{ type: "bootstrap-bye" }]);
    } catch {
      // best effort
    }
    console.log(`[mesh-random] close ${this.label}`);
  }

  handleMessage(message) {
    if (message?.type === "bootstrap-offer" && message.peer) {
      const answer = makeSignal(this.from, message.peer, "answer");
      this.pending.push({
        signal: answer,
        type: "bootstrap-answer",
      });
      console.log(`[mesh-random] answer ${this.from.slice(0, 10)} -> ${String(message.peer).slice(0, 10)}`);
      return;
    }

    if (message?.type === "bootstrap-answer" && message.peer) {
      console.log(`[mesh-random] answered ${this.from.slice(0, 10)} <- ${String(message.peer).slice(0, 10)}`);
    }
  }
}

function randomPeerPair(peers) {
  const fromIndex = crypto.randomInt(peers.length);
  let toIndex = crypto.randomInt(peers.length);
  if (toIndex === fromIndex) toIndex = (toIndex + 1) % peers.length;
  return [peers[fromIndex], peers[toIndex]];
}

async function runOnce() {
  const peers = Array.from({ length: Math.max(2, config.count) }, randomAddress);
  const deadline = Date.now() + config.keepMs;
  const sessions = [];
  const sessionTasks = [];

  console.log("[mesh-random] mode once");
  console.log("[mesh-random] target", config.url);
  console.log("[mesh-random] count", config.count);
  console.log("[mesh-random] lobby/table", `${config.lobbyId}/${config.tableId}`);

  for (let index = 0; index < config.count; index += 1) {
    const from = peers[index % peers.length];
    const to = peers[(index + 1) % peers.length];
    const session = new MeshSession(from, to, index);
    sessions.push(session);
    sessionTasks.push(session.runUntil(deadline));
    if (config.intervalMs > 0) await sleep(config.intervalMs);
  }

  console.log(`[mesh-random] keeping ${sessions.length} sessions polling for ${config.keepMs}ms`);
  await sleep(config.keepMs);

  for (const session of sessions) session.close();
  await Promise.allSettled(sessionTasks);
  console.log("[mesh-random] done");
}

async function runLoop() {
  const peerCount = Math.max(8, config.maxSessions * 2);
  const peers = Array.from({ length: peerCount }, randomAddress);
  const sessions = new Set();
  let index = 0;
  let stopping = false;

  function closeAll() {
    if (stopping) return;
    stopping = true;
    console.log("[mesh-random] stopping, closing open sessions...");
    for (const session of sessions) session.close();
  }

  process.once("SIGINT", closeAll);
  process.once("SIGTERM", closeAll);

  console.log("[mesh-random] mode loop");
  console.log("[mesh-random] target", config.url);
  console.log("[mesh-random] open rate", `${config.minOpenPerSecond}-${config.maxOpenPerSecond}/sec`);
  console.log("[mesh-random] max sessions", config.maxSessions);
  console.log("[mesh-random] close chance", config.randomCloseChance);
  console.log("[mesh-random] lobby/table", `${config.lobbyId}/${config.tableId}`);

  let lastCloseTick = Date.now();
  while (!stopping) {
    const opensThisSecond = Math.min(
      Math.max(0, config.maxSessions - sessions.size),
      randomInt(config.minOpenPerSecond, config.maxOpenPerSecond),
    );

    for (let openIndex = 0; openIndex < opensThisSecond && !stopping; openIndex += 1) {
      const [from, to] = randomPeerPair(peers);
      const session = new MeshSession(from, to, index);
      sessions.add(session);
      session.runUntil(Date.now() + config.keepMs).finally(() => sessions.delete(session));

      index += 1;
      await sleep(Math.max(50, Math.floor(1000 / Math.max(1, opensThisSecond))));
    }

    const now = Date.now();
    if (now - lastCloseTick >= config.closeEveryMs) {
      lastCloseTick = now;
      for (const session of [...sessions]) {
        if (Math.random() < config.randomCloseChance) {
          session.close();
          sessions.delete(session);
        }
      }
      console.log(`[mesh-random] pool open=${sessions.size}`);
    }

    await sleep(100);
  }

  await sleep(500);
  console.log("[mesh-random] done");
}

async function main() {
  if (config.loop) {
    await runLoop();
    return;
  }
  await runOnce();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
