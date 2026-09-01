import { normalizeAddress, shortAddress } from "./address.mjs";

const activityWindowMs = 48 * 60 * 60 * 1000;
const activityBucketMs = 60 * 60 * 1000;

export class LocalPeerRegistry {
  constructor(config) {
    this.config = config;
    this.peers = new Map();
    this.recentEvents = [];
  }

  remember(peer, { lobbyId = "", tableId = "", kind = "activity" } = {}) {
    const address = normalizeAddress(peer);
    if (!address) return;

    const now = Date.now();
    const current = this.peers.get(address) ?? {
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
    current.lobbyId = lobbyId || current.lobbyId;
    current.tableId = tableId || current.tableId;
    this.peers.set(address, current);
    this.recentEvents.push({ address, at: now, kind, lobbyId, tableId });
    this.prune(now);
  }

  markDisconnected(peer) {
    const address = normalizeAddress(peer);
    const current = this.peers.get(address);
    if (current) current.status = "DISCONNECTED";
  }

  owns(peer) {
    return this.peers.has(normalizeAddress(peer));
  }

  activeAddresses() {
    const cutoff = Date.now() - this.config.longPollTimeoutMs * 2;
    return [...this.peers.values()]
      .filter((peer) => peer.status === "ACTIVE" && peer.lastSeenAt >= cutoff)
      .map((peer) => peer.address);
  }

  snapshot(waitingKeys = new Set(), queuedPeerCount = 0) {
    const now = Date.now();
    this.prune(now);
    const bucketStart = Math.floor(now / activityBucketMs) * activityBucketMs - 47 * activityBucketMs;
    const buckets = Array.from({ length: 48 }, (_, index) => ({
      startedAt: bucketStart + index * activityBucketMs,
      count: 0,
    }));

    for (const event of this.recentEvents) {
      const index = Math.floor((event.at - bucketStart) / activityBucketMs);
      if (index >= 0 && index < buckets.length) buckets[index].count += 1;
    }

    const peers = [...this.peers.values()]
      .sort((a, b) => b.lastSeenAt - a.lastSeenAt)
      .map((peer) => ({
        address: peer.address,
        display: shortAddress(peer.address),
        count: peer.count,
        active: [...waitingKeys].some((key) => key.endsWith(`:${peer.address}`)),
        status: peer.status,
        firstSeenAt: peer.firstSeenAt,
        lastSeenAt: peer.lastSeenAt,
        lobbyId: peer.lobbyId,
        tableId: peer.tableId,
      }));

    return {
      generatedAt: now,
      windowHours: 48,
      activeCount: waitingKeys.size,
      recentPeerCount: peers.length,
      eventCount48h: this.recentEvents.length,
      queuedPeerCount,
      peers,
      buckets,
    };
  }

  prune(now = Date.now()) {
    const cutoff = now - activityWindowMs;
    while (this.recentEvents.length > 0 && this.recentEvents[0].at < cutoff) {
      this.recentEvents.shift();
    }
    for (const [address, peer] of this.peers.entries()) {
      if (peer.lastSeenAt < cutoff) this.peers.delete(address);
    }
  }
}
