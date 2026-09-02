# Private Mesh Database

Private Mesh uses Postgres as the shared Saga for client peer connection flows.

Start a local Postgres:

```sh
npm run db:up
```

Then run migrations:

```sh
npm run db:migrate:docker
```

The local database listens on `127.0.0.1:5432`.

If your local volume already contains older experimental migrations, reset it:

```sh
npm run db:reset
```

This deletes the local `private-mesh-postgres-18` Docker volume and recreates the
schema from the current migrations.

The migration runner uses `psql`; runtime database access uses the Node `pg`
client. `npm run db:migrate:docker` uses `psql` inside the compose container. If
you have `psql` installed locally, you can also run:

```sh
npm run db:migrate:local
```

Database host, port, name, and worker settings live in
`lib/mesh/config/dev.mjs` and `lib/mesh/config/main.mjs`. The only environment
values used by mesh configuration are `PRIVATE_MESH_CONFIG`,
`PRIVATE_MESH_POSTGRES_USER`, and `PRIVATE_MESH_POSTGRES_PASSWORD`.

The migrations create:

- `private_mesh.mesh_nodes`: active mesh-server instances and their deterministic node identities.
- `private_mesh.peer_connection_flows`: peer announce flows, SDP offer/answer data, MST state, processor ownership, and terminal connection state.

Postgres notifications:

- Mesh nodes should `LISTEN private_mesh_events`.
- Notification payloads are compact wakeups only; mesh nodes should re-query Postgres for durable state.
- `private_mesh.register_mesh_node(...)` emits `mesh-node-announced`.
- Peer connection flow changes emit `peer-flow-changed`.

Two-node local Saga test:

```sh
npm run mesh:test:up
npm run test:postgres-saga
npm run mesh:test:down
```
