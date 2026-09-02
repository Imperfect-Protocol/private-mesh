#!/usr/bin/env node

import { spawn } from "node:child_process";
import { MeshConfig } from "../lib/mesh/config/index.mjs";

const config = new MeshConfig();
const child = spawn(
  "docker",
  [
    "compose",
    "-f",
    "docker-compose.db.yml",
    "exec",
    "postgres",
    "psql",
    "-U",
    config.postgres.user,
    "-d",
    config.postgres.database,
  ],
  {
    stdio: "inherit",
  }
);

child.on("error", (error) => {
  if (error.code === "ENOENT") {
    console.error("docker was not found. Start Docker Desktop or install the Docker CLI.");
  } else {
    console.error(error);
  }
  process.exit(1);
});

child.on("close", (code) => {
  process.exit(code ?? 0);
});
