import crypto from "node:crypto";
import { peerKey } from "./address.mjs";
import { sendJson } from "./http.mjs";

export class LocalOutbox {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.queues = new Map();
    this.waiters = new Map();
  }

  enqueue({ lobbyId, tableId, recipient, message }) {
    const key = peerKey(lobbyId, tableId, recipient);
    const queuedMessage = {
      ...message,
      id: message.id ?? crypto.randomUUID(),
      queuedAt: Date.now(),
      expiresAt: Date.now() + this.config.messageTtlMs,
    };

    const messages = this.pruneQueue(key);
    messages.push(queuedMessage);
    this.queues.set(key, messages.slice(-this.config.maxQueuePerPeer));
    this.flush(key);
    this.logger.info("outbox.enqueue", { key, type: message.type });
  }

  drain(lobbyId, tableId, peer) {
    const key = peerKey(lobbyId, tableId, peer);
    const messages = this.pruneQueue(key);
    this.queues.delete(key);
    return messages.map(({ expiresAt: _expiresAt, ...message }) => message);
  }

  wait({ lobbyId, tableId, peer, res, onTimeout }) {
    const key = peerKey(lobbyId, tableId, peer);
    const existing = this.waiters.get(key);
    if (existing) this.finish(key, [], true);

    const timer = setTimeout(() => {
      onTimeout?.();
      this.finish(key, [], true);
    }, this.config.longPollTimeoutMs);

    this.waiters.set(key, { res, timer });
    return key;
  }

  releaseWaiter(key, res) {
    const waiter = this.waiters.get(key);
    if (waiter?.res !== res) return false;
    this.waiters.delete(key);
    clearTimeout(waiter.timer);
    return true;
  }

  closeAll() {
    for (const key of [...this.waiters.keys()]) {
      this.finish(key, [], true);
    }
  }

  waitingKeys() {
    return new Set(this.waiters.keys());
  }

  queuedPeerCount() {
    return this.queues.size;
  }

  pruneQueue(key, now = Date.now()) {
    const messages = this.queues.get(key) ?? [];
    const fresh = messages.filter((message) => Number(message.expiresAt ?? 0) > now);
    if (fresh.length > 0) {
      this.queues.set(key, fresh);
    } else {
      this.queues.delete(key);
    }
    return fresh;
  }

  flush(key) {
    if (!this.waiters.has(key)) return;
    const messages = this.pruneQueue(key);
    if (messages.length > 0) {
      this.queues.delete(key);
      this.finish(key, messages.map(({ expiresAt: _expiresAt, ...message }) => message), false);
    }
  }

  finish(key, messages, timeout) {
    const waiter = this.waiters.get(key);
    if (!waiter) return false;

    this.waiters.delete(key);
    clearTimeout(waiter.timer);
    sendJson(waiter.res, 200, {
      messages,
      timeout,
      now: Date.now(),
    });
    return true;
  }
}
