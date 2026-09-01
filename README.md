# Private Mesh

Next.js Grid frontend plus a Postgres-backed HTTP long-poll bootstrap relay for
Private Poker browser peers.

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

Local Postgres:

```sh
npm run db:up
npm run db:migrate:docker
```

If this local dev database has older experimental migrations in it, reset the
Docker volume and recreate the schema:

```sh
npm run db:reset
```

Two-node Postgres Saga smoke test:

```sh
npm run mesh:test:up
npm run test:postgres-saga
npm run mesh:test:down
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
PRIVATE_MESH_MST_INTERVAL_MS=3000
PRIVATE_MESH_HEARTBEAT_INTERVAL_MS=10000
PRIVATE_MESH_PROCESSING_TIMEOUT_MS=15000
PRIVATE_MESH_ENABLE_LOGGING=0
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
```
