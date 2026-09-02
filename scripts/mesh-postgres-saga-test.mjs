#!/usr/bin/env node

import crypto from "node:crypto";

const defaults = {
  lobbyId: "postgres-saga-test",
  tableId: `table-${Date.now()}`,
  nodeXUrl: "http://127.0.0.1:3001/mesh",
  nodeYUrl: "http://127.0.0.1:3002/mesh",
  timeoutMs: 20_000,
};

const config = {
  lobbyId: argValue("lobby-id", defaults.lobbyId),
  tableId: argValue("table-id", defaults.tableId),
  nodeXUrl: argValue("node-x-url", defaults.nodeXUrl),
  nodeYUrl: argValue("node-y-url", defaults.nodeYUrl),
  timeoutMs: Number(argValue("timeout-ms", defaults.timeoutMs)),
};

const peerA = randomAddress();
const peerB = randomAddress();
const completed = new Set();

class SimulatedPeer {
  constructor({ label, address, target, meshUrl }) {
    this.label = label;
    this.address = address;
    this.target = target;
    this.meshUrl = meshUrl;
    this.pending = [{
      type: "bootstrap-announce",
      peer: address,
      lookingFor: [target],
    }];
    this.stopped = false;
  }

  async run() {
    while (!this.stopped) {
      const outbound = this.pending.splice(0);
      const response = await pollMesh({
        meshUrl: this.meshUrl,
        peer: this.address,
        messages: outbound,
      });

      for (const message of response.messages) {
        await this.handleMessage(message);
      }
    }
  }

  stop() {
    this.stopped = true;
  }

  async handleMessage(message) {
    if (message.type === "trigger-offer") {
      console.log(`[${this.label}] trigger-offer -> ${shortAddress(message.target)}`);
      this.pending.push({
        type: "bootstrap-interest",
        signal: signal({
          from: this.address,
          to: message.target,
          kind: "offer",
          flowId: message.flowId,
        }),
      });
      return;
    }

    if (message.type === "bootstrap-offer") {
      console.log(`[${this.label}] bootstrap-offer <- ${shortAddress(message.peer)}`);
      this.pending.push({
        type: "bootstrap-answer",
        signal: signal({
          from: this.address,
          to: message.peer,
          kind: "accept",
          flowId: message.flowId,
        }),
      });
      return;
    }

    if (message.type === "bootstrap-answer") {
      console.log(`[${this.label}] bootstrap-answer <- ${shortAddress(message.peer)}`);
      this.pending.push({
        type: "link-connected",
        from: this.address,
        to: message.peer,
      });
      completed.add(`${this.address}:${message.peer}:primary`);
      return;
    }

    if (message.type === "connect-p2p") {
      console.log(`[${this.label}] connect-p2p -> ${shortAddress(message.target)}`);
      this.pending.push({
        type: "link-connected",
        from: this.address,
        to: message.target,
      });
      completed.add(`${this.address}:${message.target}:reverse`);
    }
  }
}

async function main() {
  console.log(`Peer A ${peerA} on Node X ${config.nodeXUrl}`);
  console.log(`Peer B ${peerB} on Node Y ${config.nodeYUrl}`);

  const a = new SimulatedPeer({
    label: "Peer A",
    address: peerA,
    target: peerB,
    meshUrl: config.nodeXUrl,
  });
  const b = new SimulatedPeer({
    label: "Peer B",
    address: peerB,
    target: peerA,
    meshUrl: config.nodeYUrl,
  });

  const tasks = [a.run()];
  await sleep(300);
  tasks.push(b.run());
  const startedAt = Date.now();
  while (Date.now() - startedAt < config.timeoutMs) {
    if (hasSuccess()) break;
    await sleep(250);
  }

  a.stop();
  b.stop();
  await Promise.allSettled(tasks);

  if (!hasSuccess()) {
    console.error("Postgres Saga test did not complete.");
    process.exit(1);
  }

  console.log("Postgres Saga test completed.");
}

function hasSuccess() {
  return (
    completed.has(`${peerA}:${peerB}:primary`) &&
    completed.has(`${peerB}:${peerA}:reverse`)
  ) || (
    completed.has(`${peerB}:${peerA}:primary`) &&
    completed.has(`${peerA}:${peerB}:reverse`)
  );
}

async function pollMesh({ meshUrl, peer, messages }) {
  const response = await fetch(meshUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      lobbyId: config.lobbyId,
      tableId: config.tableId,
      peer,
      messages,
    }),
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body?.error ?? `mesh returned ${response.status}`);
  return body;
}

function signal({ from, to, kind, flowId }) {
  return {
    originAddress: from,
    targetAddress: to,
    lobbyId: config.lobbyId,
    tableId: config.tableId,
    encryptedData: `0x${crypto.randomBytes(96).toString("hex")}`,
    kind,
    flowId,
  };
}

function argValue(name, fallback) {
  const prefix = `--${name}=`;
  const inline = process.argv.find((arg) => arg.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && process.argv[index + 1]) return process.argv[index + 1];
  return fallback;
}

function randomAddress() {
  return `0x${crypto.randomBytes(20).toString("hex")}`;
}

function shortAddress(value) {
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
