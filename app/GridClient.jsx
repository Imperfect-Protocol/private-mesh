"use client";

import { useEffect, useMemo, useState } from "react";

const emptyStats = {
  activeCount: 0,
  buckets: [],
  eventCount48h: 0,
  generatedAt: 0,
  peers: [],
  queuedPeerCount: 0,
  recentPeerCount: 0,
  windowHours: 48
};

function shortTime(value) {
  if (!value) return "--:--";
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(value));
}

function fullTime(value) {
  if (!value) return "Waiting for signal";
  return new Intl.DateTimeFormat("en", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "short"
  }).format(new Date(value));
}

export default function GridClient() {
  const [stats, setStats] = useState(emptyStats);
  const [status, setStatus] = useState("syncing");

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
      startedAt: Date.now() - (47 - index) * 60 * 60 * 1000
    }));

  return (
    <section className="grid-console">
      <div className="grid-console__header">
        <div>
          <p className="grid-kicker">Mesh telemetry</p>
          <h1>The Grid</h1>
          <span>Recent peer-to-peer bootstrap traces from Private Mesh.</span>
        </div>
        <div className={`grid-status grid-status--${status}`}>
          <i />
          <span>{status}</span>
        </div>
      </div>

      <div className="grid-stats">
        <article>
          <span>Active polls</span>
          <strong>{stats.activeCount}</strong>
        </article>
        <article>
          <span>Peers seen</span>
          <strong>{stats.recentPeerCount}</strong>
        </article>
        <article>
          <span>Signals / 48h</span>
          <strong>{stats.eventCount48h}</strong>
        </article>
      </div>

      <section className="grid-chart" aria-label="Mesh signal count over the last 48 hours">
        <div className="grid-chart__top">
          <h2>Last 48 Hours</h2>
          <span>Updated {fullTime(stats.generatedAt)}</span>
        </div>
        <div className="grid-chart__bars">
          {chartBuckets.map((bucket, index) => (
            <div className="grid-chart__bar-wrap" key={`${bucket.startedAt}-${index}`}>
              <span
                className="grid-chart__bar"
                style={{ height: `${Math.max(4, (bucket.count / maxBucketCount) * 100)}%` }}
                title={`${shortTime(bucket.startedAt)} - ${bucket.count}`}
              />
            </div>
          ))}
        </div>
        <div className="grid-chart__axis">
          <span>-48h</span>
          <span>now</span>
        </div>
      </section>

      <section className="grid-peers" aria-label="Recent mesh peer addresses">
        <div className="grid-chart__top">
          <h2>Recent Peer Addresses</h2>
          <span>{stats.peers.length} visible</span>
        </div>
        <div className="grid-peer-list">
          {stats.peers.length > 0 ? (
            stats.peers.map((peer) => (
              <article className="grid-peer" key={peer.address}>
                <div>
                  <strong>{peer.address}</strong>
                  <span>{peer.active ? "active" : "seen"} / {peer.count} traces / {fullTime(peer.lastSeenAt)}</span>
                </div>
                <i>{peer.display}</i>
              </article>
            ))
          ) : (
            <article className="grid-empty">Waiting for mesh bootstrap traffic.</article>
          )}
        </div>
      </section>
    </section>
  );
}
