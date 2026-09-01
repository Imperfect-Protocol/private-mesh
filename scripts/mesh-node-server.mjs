#!/usr/bin/env node

import http from "node:http";
import { closeMeshWaiters, handleMeshRequest, logConfiguration } from "../lib/mesh.mjs";

const port = Number(process.env.PORT || 3000);
const host = process.env.HOST || "127.0.0.1";

const server = http.createServer((req, res) => {
  handleMeshRequest(req, res)
    .then((handled) => {
      if (handled) return;
      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      res.end("Not Found\n");
    })
    .catch((error) => {
      console.error(error);
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      }
      res.end("Internal Server Error\n");
    });
});

server.timeout = 0;
server.requestTimeout = 0;
server.headersTimeout = 65_000;
server.keepAliveTimeout = 10_000;

server.listen(port, host, () => {
  console.log(`Private Mesh node listening on http://${host}:${port}`);
  console.log(`Mesh API on http://${host}:${port}/mesh`);
  logConfiguration();
});

function shutdown() {
  closeMeshWaiters();
  server.close(() => process.exit(0));
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
