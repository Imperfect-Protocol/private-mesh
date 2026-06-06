# Private Mesh

Private Mesh is a tiny generic signal relay webservice for browser peers.

It exposes:

- `PUT /signal` for submitting one opaque signal frame
- `GET /signal/stream` for receiving signals over SSE
- `GET /signal` for polling
- a minimal status endpoint at `/mesh.json`
- a black one-page frontend with a short teal splash

Signals are treated as opaque hex frames. The first 20 bytes are the recipient address. The rest is encrypted payload.

## Run

```sh
npm install
npm run start
```

Defaults:

- UI: `http://127.0.0.1:8787`
- send: `PUT /signal`
- stream: `GET /signal/stream?recipient=<address>`
- poll: `GET /signal?recipient=<address>`

## Signal Frames

```text
0x<20-byte-recipient-address><encrypted-payload>
```

Send one or more frames. Each line is delivered independently:

```text
0x<20-byte-recipient-1><encrypted-payload-1>
0x<20-byte-recipient-2><encrypted-payload-2>
```

Example:

```sh
curl -X PUT http://127.0.0.1:8787/signal \
  --data $'0x1111111111111111111111111111111111111111abcdef\n0x2222222222222222222222222222222222222222fedcba'
```

## Configuration

```sh
PRIVATE_MESH_HTTP_HOST=0.0.0.0
PRIVATE_MESH_HTTP_PORT=8787
PRIVATE_MESH_MAX_BODY_BYTES=1048576
PRIVATE_MESH_MAX_RETAINED=200
PRIVATE_MESH_SIGNAL_TTL_MS=120000
```
