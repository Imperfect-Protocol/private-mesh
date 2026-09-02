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

Runtime settings live in `lib/mesh/config/dev.mjs` and
`lib/mesh/config/main.mjs`.

The only mesh configuration selector is:

- `PRIVATE_MESH_CONFIG=dev`
- `PRIVATE_MESH_CONFIG=main`

Postgres credentials can be supplied with:

- `PRIVATE_MESH_POSTGRES_USER`
- `PRIVATE_MESH_POSTGRES_PASSWORD`

Everything else, including database host/port, worker timings, queue limits, and
logging, should be changed in the config files.
