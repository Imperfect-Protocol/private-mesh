"use client";

import { useEffect, useMemo, useState } from "react";

const emptyStats = {
  activeCount: 0,
  buckets: [],
  eventCount48h: 0,
  generatedAt: 0,
  openInterests: 0,
  peers: [],
  queuedPeerCount: 0,
  recentPeerCount: 0,
  windowHours: 48,
};

function shortTime(value) {
  if (!value) return "--:--";
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function fullTime(value) {
  if (!value) return "Waiting for signal";
  return new Intl.DateTimeFormat("en", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "short",
    second: "2-digit",
  }).format(new Date(value));
}

export default function GridClient() {
  const [stats, setStats] = useState(emptyStats);
  const [status, setStatus] = useState("syncing");
  const [searchQuery, setSearchQuery] = useState("");
  const [filterActiveOnly, setFilterActiveOnly] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadStats() {
      try {
        const response = await fetch("/mesh/grid", { cache: "no-store" });
        if (!response.ok) throw new Error(`mesh grid returned ${response.status}`);
        const nextStats = await response.json();
        if (!cancelled) {
          setStats(nextStats);
          setStatus("live");
        }
      } catch {
        if (!cancelled) setStatus("offline");
      }
    }

    loadStats();
    const timer = window.setInterval(loadStats, 2_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  const maxBucketCount = useMemo(
    () => Math.max(1, ...stats.buckets.map((bucket) => bucket.count)),
    [stats.buckets]
  );

  const chartBuckets = stats.buckets.length > 0
    ? stats.buckets
    : Array.from({ length: 48 }, (_, index) => ({
      count: 0,
      startedAt: Date.now() - (47 - index) * 60 * 60 * 1000,
    }));

  const filteredPeers = useMemo(() => {
    return (stats.peers ?? []).filter((peer) => {
      if (filterActiveOnly && !peer.active) return false;
      if (!searchQuery.trim()) return true;

      const q = searchQuery.toLowerCase().trim();
      return (
        peer.address?.toLowerCase().includes(q) ||
        peer.display?.toLowerCase().includes(q) ||
        peer.tableId?.toLowerCase().includes(q) ||
        peer.lobbyId?.toLowerCase().includes(q)
      );
    });
  }, [stats.peers, searchQuery, filterActiveOnly]);

  return (
    <section className="grid-console">
      {/* Console Header */}
      <div className="grid-console__header">
        <div>
          <p className="grid-kicker">Mesh Telemetry & Graph Monitor</p>
          <h1>The Grid</h1>
          <span>Real-time orchestration traces, peer connections, and Prim's MST bootstrap telemetry.</span>
        </div>
        <div className={`grid-status grid-status--${status}`}>
          <i />
          <span>{status}</span>
        </div>
      </div>

      {/* Primary KPI Grid */}
      <div className="grid-stats">
        <article>
          <span>Active Long-Polls</span>
          <strong>{stats.activeCount}</strong>
          <small>Open HTTP waiters</small>
        </article>
        <article>
          <span>Queued Messages</span>
          <strong>{stats.queuedPeerCount}</strong>
          <small>Pending handshake payloads</small>
        </article>
        <article>
          <span>Open Interests / Edges</span>
          <strong>{stats.openInterests}</strong>
          <small>Candidate Graph Links</small>
        </article>
        <article>
          <span>Total Peers Seen</span>
          <strong>{stats.recentPeerCount}</strong>
          <small>Registered nodes</small>
        </article>
        <article>
          <span>48h Bootstrap Events</span>
          <strong>{stats.eventCount48h}</strong>
          <small>Total network traces</small>
        </article>
      </div>

      {/* Engine Config / Topology Bar */}
      <div className="grid-engine-info">
        <div className="grid-engine-info__item">
          <span>Batch Window:</span> <strong>3.0s (Prim's MST)</strong>
        </div>
        <div className="grid-engine-info__item">
          <span>Poll Timeout:</span> <strong>30.0s</strong>
        </div>
        <div className="grid-engine-info__item">
          <span>Handshake Step:</span> <strong>6.0s max</strong>
        </div>
        <div className="grid-engine-info__item">
          <span>Last Snapshot:</span> <strong>{shortTime(stats.generatedAt)}</strong>
        </div>
      </div>

      {/* 48-Hour Signal Activity Chart */}
      <section className="grid-chart" aria-label="Mesh signal count over the last 48 hours">
        <div className="grid-chart__top">
          <h2>48-Hour Activity Histogram</h2>
          <span>Updated {fullTime(stats.generatedAt)}</span>
        </div>
        <div className="grid-chart__bars">
          {chartBuckets.map((bucket, index) => (
            <div className="grid-chart__bar-wrap" key={`${bucket.startedAt}-${index}`}>
              <span
                className="grid-chart__bar"
                style={{ height: `${Math.max(4, (bucket.count / maxBucketCount) * 100)}%` }}
                title={`${shortTime(bucket.startedAt)} — ${bucket.count} events`}
              />
            </div>
          ))}
        </div>
        <div className="grid-chart__axis">
          <span>-48h</span>
          <span>-24h</span>
          <span>now</span>
        </div>
      </section>

      {/* Peer Telemetry Table & Filters */}
      <section className="grid-peers" aria-label="Recent mesh peer addresses">
        <div className="grid-chart__top grid-peers__header">
          <div>
            <h2>Peer Registry & Topology Nodes</h2>
            <span>{filteredPeers.length} of {stats.peers.length} peers shown</span>
          </div>

          <div className="grid-peers__controls">
            <input
              type="text"
              placeholder="Filter by address, table, or lobby..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="grid-input"
            />
            <label className="grid-checkbox-label">
              <input
                type="checkbox"
                checked={filterActiveOnly}
                onChange={(e) => setFilterActiveOnly(e.target.checked)}
              />
              <span>Active only</span>
            </label>
          </div>
        </div>

        <div className="grid-peer-table-wrap">
          {filteredPeers.length > 0 ? (
            <table className="grid-peer-table">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Peer Address</th>
                  <th>Lobby / Table</th>
                  <th>Traces</th>
                  <th>First Seen</th>
                  <th>Last Active</th>
                </tr>
              </thead>
              <tbody>
                {filteredPeers.map((peer) => (
                  <tr key={peer.address} className={peer.active ? "peer-row--active" : ""}>
                    <td>
                      <span className={`grid-badge ${peer.active ? "grid-badge--active" : "grid-badge--idle"}`}>
                        {peer.active ? "ACTIVE" : "SEEN"}
                      </span>
                    </td>
                    <td>
                      <div className="grid-peer-address">
                        <strong>{peer.display}</strong>
                        <small>{peer.address}</small>
                      </div>
                    </td>
                    <td>
                      {peer.lobbyId || peer.tableId ? (
                        <span className="grid-tag">
                          L:{peer.lobbyId || "--"} / T:{peer.tableId || "--"}
                        </span>
                      ) : (
                        <span className="grid-tag grid-tag--muted">Unassigned</span>
                      )}
                    </td>
                    <td>
                      <strong>{peer.count}</strong>
                    </td>
                    <td>{fullTime(peer.firstSeenAt)}</td>
                    <td>{fullTime(peer.lastSeenAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="grid-empty">
              {searchQuery || filterActiveOnly
                ? "No peer addresses match your current filter criteria."
                : "Waiting for mesh bootstrap traffic..."}
            </div>
          )}
        </div>
      </section>
    </section>
  );
}