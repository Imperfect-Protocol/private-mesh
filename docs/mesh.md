# Private Mesh Saga

Private Mesh is a small HTTP long-poll signaling carrier for browser peers. The
durable coordination state now lives in Postgres so more than one mesh-server
instance can participate in the same table.

## Runtime Shape

- Browser peers long-poll their nearest mesh node at `/mesh`.
- Mesh nodes remember only local peer ownership and open long-poll responses in
  memory.
- Connection flows, SDP payloads, MST state, processors, timestamps, and terminal
  results are stored in `private_mesh.peer_connection_flows`.
- Postgres `LISTEN private_mesh_events` wakes each mesh node after flow changes.
  Workers then re-query Postgres and act only on rows involving their local
  connected peers.

## Workers

- `HeartbeatWorker`: registers the mesh node in Postgres and refreshes
  `last_seen_at`.
- `NotificationWorker`: listens for Postgres notifications and wakes the local
  worker loop.
- `MstWorker`: periodically reads announced flows involving local peers, builds a
  table-scoped MST plan, and moves selected rows to `RequestOffer`.
- `FlowReactionWorker`: converts durable row states into local outbox messages for
  connected browser peers.

## Flow States

- `Announced`: a peer wants to connect to another peer.
- `RequestOffer`: the MST selected this row and the `from_peer` should create an
  offer.
- `AwaitingP2P`: the row was seen but not selected as the primary MST edge.
- `Offer`: encrypted SDP offer is stored in `data`.
- `Answer`: encrypted SDP answer is stored in `data`.
- `Connected`: the primary offer/answer path succeeded.
- `ConnectP2P`: the reverse row should be completed directly by the reverse peer.
- `ConnectedP2P`: the reverse P2P instruction succeeded.
- `Failed`: a peer reported that the link failed.

## Two-Node Example

Peer A is connected to Node X, and Peer B is connected to Node Y.

1. A posts `bootstrap-announce` looking for B.
2. Node X inserts `A -> B` as `Announced`.
3. Node X's MST worker selects `A -> B` and updates it to `RequestOffer`.
4. Node X asks A to create an offer with `trigger-offer`.
5. A posts `bootstrap-interest`; Node X stores the encrypted offer and marks the
   row `Offer`.
6. Node Y wakes from Postgres notification, sees `Offer` with local `to_peer = B`,
   and delivers `bootstrap-offer` to B.
7. B posts `bootstrap-answer`; Node Y stores the encrypted answer and marks the
   row `Answer`.
8. Node X wakes, sees `Answer` with local `from_peer = A`, and delivers the
   answer to A.
9. A reports `link-connected`; Node X marks `A -> B` as `Connected` and creates or
   updates `B -> A` as `ConnectP2P`.
10. Node Y wakes, sees `ConnectP2P` with local `from_peer = B`, and tells B to
    connect to A.
11. B reports `link-connected`; Node Y marks `B -> A` as `ConnectedP2P`.

SDP data is cleared when rows become terminal so Postgres keeps only the durable
Saga outcome.
