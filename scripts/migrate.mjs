#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MeshConfig } from "../lib/mesh/config/index.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const migrationsDir = path.join(root, "db", "migrations");
const config = new MeshConfig();
const useDockerPsql = process.argv.includes("--docker");

function runPsql(args, input, { capture = false } = {}) {
  return new Promise((resolve, reject) => {
    const command = useDockerPsql ? "docker" : "psql";
    const commandArgs = useDockerPsql
      ? ["compose", "-f", "docker-compose.db.yml", "exec", "-T", "postgres", "psql", "-U", config.postgres.user, "-d", config.postgres.database, "-v", "ON_ERROR_STOP=1", ...args]
      : [config.databaseUrl, "-v", "ON_ERROR_STOP=1", ...args];

    const child = spawn(command, commandArgs, {
      stdio: ["pipe", capture ? "pipe" : "inherit", "inherit"],
      cwd: root,
    });

    let stdout = "";
    if (capture) {
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        stdout += chunk;
      });
    }

    child.on("error", (error) => {
      if (error.code === "ENOENT") {
        reject(new Error(useDockerPsql
          ? "docker was not found. Start Docker Desktop or install the Docker CLI."
          : "psql was not found. Install PostgreSQL client tools or run with --docker."
        ));
      } else {
        reject(error);
      }
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(`psql exited with status ${code}`));
      }
    });

    child.stdin.end(input);
  });
}

function runDockerCompose(args, { capture = false } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", ["compose", "-f", "docker-compose.db.yml", ...args], {
      stdio: ["ignore", capture ? "pipe" : "inherit", capture ? "pipe" : "inherit"],
      cwd: root,
    });

    let stdout = "";
    let stderr = "";
    if (capture) {
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        stdout += chunk;
      });
      child.stderr.on("data", (chunk) => {
        stderr += chunk;
      });
    }

    child.on("error", (error) => {
      if (error.code === "ENOENT") {
        reject(new Error("docker was not found. Start Docker Desktop or install the Docker CLI."));
      } else {
        reject(error);
      }
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(new Error(capture && stderr.trim() ? stderr.trim() : `docker compose exited with status ${code}`));
      }
    });
  });
}

async function waitForDockerPostgres() {
  if (!useDockerPsql) return;

  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      await runDockerCompose(
        ["exec", "-T", "postgres", "pg_isready", "-U", config.postgres.user, "-d", config.postgres.database],
        { capture: true }
      );
      return;
    } catch (error) {
      if (attempt === 30) throw error;
      await sleep(500);
    }
  }
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

await waitForDockerPostgres();

await runPsql([], `
CREATE SCHEMA IF NOT EXISTS private_mesh;

CREATE TABLE IF NOT EXISTS private_mesh.schema_migrations (
  filename text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
`);

const appliedOutput = await runPsql(
  ["-X", "-q", "-t", "-A"],
  "SELECT filename || ' ' || checksum FROM private_mesh.schema_migrations ORDER BY filename;",
  { capture: true }
);

const applied = new Map(
  appliedOutput
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [filename, checksum] = line.split(" ");
      return [filename, checksum];
    })
);

const migrationFiles = (await readdir(migrationsDir))
  .filter((file) => file.endsWith(".sql"))
  .sort();

for (const filename of migrationFiles) {
  const fullPath = path.join(migrationsDir, filename);
  const sql = await readFile(fullPath, "utf8");
  const checksum = cryptoHash(sql);

  if (applied.has(filename)) {
    if (applied.get(filename) !== checksum) {
      throw new Error(`Applied migration checksum changed: ${filename}`);
    }
    console.log(`Skipping ${filename}`);
    continue;
  }

  console.log(`Applying ${filename}`);
  await runPsql([], `
BEGIN;

${sql}

INSERT INTO private_mesh.schema_migrations (filename, checksum)
VALUES (${sqlString(filename)}, ${sqlString(checksum)});

COMMIT;
`);
}

console.log("Database migrations complete.");

function cryptoHash(value) {
  return createHash("sha256").update(value).digest("hex");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
