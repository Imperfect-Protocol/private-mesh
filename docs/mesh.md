Here is a complete breakdown of your server log output.

This log captures the server (`mesh.mjs`) orchestrating peer connections for the automated test suite (`mesh-random-requests.mjs`). It shows Prim's MST algorithm selecting core tree edges for sequential WebRTC triggering while offloading non-MST edges to P2P gossip.

---

## 🛠️ Server Startup Configuration

```javascript
{
  maxBodyBytes: 1048576,
  maxQueuePerPeer: 200,
  messageTtlMs: 120000,
  longPollTimeoutMs: 30000,
  batchIntervalMs: 3000,
  handshakeTimeoutMs: 6000,
  enableLogging: 1
}

```

* **`longPollTimeoutMs: 30000`**: Long-poll connections stay open up to 30 seconds before timing out.
* **`batchIntervalMs: 3000`**: Every 3 seconds, the batch loop runs Prim's MST algorithm on incoming peer announcements.
* **`handshakeTimeoutMs: 6000`**: Mesh allows up to 6 seconds per WebRTC handshake before moving to the next edge in the queue.

---

## 🔑 Quick Reference: What Each Log Event Means

* **`[mesh] handshake.trigger`**: Mesh selected an edge in the MST and told the origin node: *"It's your turn to send a WebRTC offer."*
* **`[mesh] p2p.route_notify`**: Mesh identified a non-MST (cycle/redundant) edge and notified the origin node: *"This route is not in the MST; establish it over P2P gossip instead."*
* **`[mesh] interest.store`**: Mesh received a `bootstrap-offer` (encrypted WebRTC SDP offer) from a client and stored it in memory.
* **`[mesh] answer.forward`**: Mesh received a `bootstrap-answer` (encrypted WebRTC SDP accept) and forwarded it back to the offerer.
* **`[mesh] handshake.complete`**: Mesh registered the WebRTC handshake as finished, releasing the queue lock to process the next MST edge.
* **`[mesh] message.enqueue`**: A signaling payload was queued into a peer's long-poll inbox (`queues`, `waiters`, and `interests` show active server state).

---

## 📜 Detailed Log Walkthrough

### 1️⃣ Table 0 (`test-table-...-0`) — 2-Edge Chain + P2P Offload

#### **Step A: Edge 1 MST Trigger & P2P Offload**

```text
[mesh] message.enqueue { key: '...:0x355d...bd6e', type: 'trigger-offer', recipient: '0x355d...bd6e' }
[mesh] handshake.trigger { from: '0x355d...bd6e', to: '0x1312...7615' }

```

> **What happened:** Prim's MST selected edge `0x355d → 0x1312`. Mesh enqueues a `trigger-offer` message to `0x355d...bd6e`.

```text
[mesh] message.enqueue { key: '...:0xa666...a0c2', type: 'p2p-route-available', recipient: '0xa666...a0c2' }
[mesh] p2p.route_notify { from: '0xa666...a0c2', to: '0x355d...bd6e' }

```

> **What happened:** Edge `0xa666 → 0x355d` was in the batch but **not** in the MST ($E_{\text{P2P}} = E \setminus E_{\text{MST}}$). Mesh flushes a `p2p-route-available` message to `0xa666` to offload this link to direct P2P gossip.

#### **Step B: WebRTC Offer / Answer for Edge 1**

```text
[mesh] message.enqueue { type: 'bootstrap-offer', recipient: '0x1312...7615' }
[mesh] interest.store { signal: { id: '...', from: '0x355d...bd6e', to: '0x1312...7615', payloadBytes: 96 } }

```

> **What happened:** Node `0x355d` received the `trigger-offer`, created a 96-byte encrypted WebRTC SDP offer, and POSTed it to Mesh. Mesh stored the interest and queued it for `0x1312`.

```text
[mesh] message.enqueue { type: 'bootstrap-answer', recipient: '0x355d...bd6e' }
[mesh] handshake.complete { from: '0x355d...bd6e', to: '0x1312...7615' }
[mesh] answer.forward { signal: { id: '...', from: '0x1312...7615', to: '0x355d...bd6e', kind: 'accept' } }

```

> **What happened:** Node `0x1312` received the offer, generated an SDP accept answer, and sent it back. Mesh marked the handshake `0x355d → 0x1312` as **complete** and forwarded the answer to `0x355d`.

#### **Step C: Queue Advances to Edge 2 (`0x1312 → 0xa666`)**

```text
[mesh] message.enqueue { type: 'trigger-offer', recipient: '0x1312...7615' }
[mesh] handshake.trigger { from: '0x1312...7615', to: '0xa666...a0c2' }
...
[mesh] interest.store { signal: { from: '0x1312...7615', to: '0xa666...a0c2' } }
[mesh] handshake.complete { from: '0x1312...7615', to: '0xa666...a0c2' }
[mesh] answer.forward { signal: { from: '0xa666...a0c2', to: '0x1312...7615', kind: 'accept' } }

```

> **What happened:** With Edge 1 connected, the queue automatically advanced. Mesh triggered Node `0x1312` to offer to Node `0xa666`, exchanged the offer/answer SDP signals, and marked the full tree complete.

---

### 2️⃣ Table 1 (`test-table-...-1`) — Larger Graph with Multiple P2P Offloads

```text
[mesh] handshake.trigger { from: '0xf1bc...6805', to: '0xe593...a589' }
[mesh] p2p.route_notify { from: '0xdb52...691b', to: '0x2cb1...af71' }
[mesh] p2p.route_notify { from: '0x8e5c...4d1c', to: '0xf1bc...6805' }

```

> **What happened:**
> 1. Prim's MST picked `0xf1bc → 0xe593` as the first spanning tree edge to trigger over WebRTC.
> 2. Two redundant cycle edges (`0xdb52 → 0x2cb1` and `0x8e5c → 0xf1bc`) were immediately offloaded as P2P gossip routes (`p2p.route_notify`).
> 
> 

The log then executes three sequential WebRTC handshakes in order:

1. `0xf1bc...6805 → 0xe593...a589` (Handshake completes)
2. `0xe593...a589 → 0x8e5c...4d1c` (Handshake completes)
3. `0x2cb1...af71 → 0xdb52...691b` (Handshake completes)

---

### 3️⃣ Table 2 (`test-table-...-2`) — Simple Pair Execution

```text
[mesh] handshake.trigger { from: '0x8f36...b38c', to: '0x9c3d...19f2' }
[mesh] p2p.route_notify { from: '0x9c3d...19f2', to: '0x8f36...b38c' }
...
[mesh] handshake.complete { from: '0x8f36...b38c', to: '0x9c3d...19f2' }

```

> **What happened:** Mesh triggers `0x8f36 → 0x9c3d` for the MST, offloads the reverse link `0x9c3d → 0x8f36` to P2P gossip, and successfully completes the WebRTC handshake.

---

### 4️⃣ Table 3 (`test-table-...-3`) — 5-Node Spanning Tree Pipeline

```text
1. [mesh] handshake.trigger { from: '0x436d...97bd', to: '0xd00e...aec1' }
   ↳ [mesh] p2p.route_notify { from: '0xd00e...aec1', to: '0x1615...17a1' }
   ↳ [mesh] p2p.route_notify { from: '0x8a49...1646', to: '0x436d...97bd' }
2. [mesh] handshake.trigger { from: '0x436d...97bd', to: '0x1615...17a1' }
3. [mesh] handshake.trigger { from: '0x1615...17a1', to: '0x1870...cdce' }
4. [mesh] handshake.trigger { from: '0x1870...cdce', to: '0xf4ef...6c09' }
5. [mesh] handshake.trigger { from: '0xf4ef...6c09', to: '0x8a49...1646' }

```

> **What happened:**
> * Mesh computes a 5-hop MST chain connecting all nodes: `0x436d → 0xd00e → 0x1615 → 0x1870 → 0xf4ef → 0x8a49`.
> * Cross-links `0xd00e → 0x1615` and `0x8a49 → 0x436d` are offloaded to P2P.
> * Mesh executes all 5 MST WebRTC handshakes sequentially step-by-step, waiting for each step's `handshake.complete` before triggering the next link. Zero STUN/ICE binding collisions occur.
> 
>