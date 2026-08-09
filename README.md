# Private Mesh

Next.js Grid frontend plus HTTP long-poll bootstrap relay for Private Poker
browser peers.

## Run

```sh
npm install
npm run bundle
npm start
```

For local iteration with the same custom server path:

```sh
npm run dev
```

Defaults:

- Grid UI: `http://127.0.0.1:3000/`
- long poll: `http://127.0.0.1:3000/mesh`
- status grid JSON: `http://127.0.0.1:3000/mesh/grid`

Random request smoke test:

```sh
npm run random -- --count 80 --keep-ms 60000
```

Configuration:

```sh
HOST=0.0.0.0
PORT=3000
PRIVATE_MESH_MAX_BODY_BYTES=1048576
PRIVATE_MESH_MAX_QUEUE_PER_PEER=200
PRIVATE_MESH_MESSAGE_TTL_MS=120000
PRIVATE_MESH_LONG_POLL_TIMEOUT_MS=5000
PRIVATE_MESH_ENABLE_LOGGING=0
```
