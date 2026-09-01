import crypto from "node:crypto";
import pg from "pg";
import { FlowState } from "./flow-states.mjs";

const { Client, Pool } = pg;

export class PostgresMeshRepository {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.pool = null;
  }

  start() {
    this.config.requireDatabase();
    if (!this.pool) {
      this.pool = new Pool({ connectionString: this.config.databaseUrl });
      this.pool.on("error", (error) => {
        this.logger.error("postgres.pool.error", error);
      });
    }
  }

  async stop() {
    if (this.pool) await this.pool.end();
    this.pool = null;
  }

  async registerMeshNode() {
    await this.query(
      `SELECT private_mesh.register_mesh_node($1, $2, $3, $4, $5, $6, $7)`,
      [
        this.config.nodeId,
        this.config.publicKey,
        this.config.instanceMessage,
        this.config.region,
        this.config.version,
        this.config.maxConnections,
        { pid: process.pid },
      ]
    );
  }

  async heartbeatMeshNode() {
    await this.query(`SELECT private_mesh.heartbeat_mesh_node($1)`, [this.config.nodeId]);
  }

  createListener() {
    this.config.requireDatabase();
    return new Client({ connectionString: this.config.databaseUrl });
  }

  async insertAnnounce({ lobbyId, tableId, fromPeer, toPeer }) {
    const flow = await this.one(
      `
      INSERT INTO private_mesh.peer_connection_flows (
        id, lobby_id, table_id, from_peer, to_peer, data, mst_state, last_update_at, metadata
      )
      VALUES ($1, $2, $3, $4, $5, NULL, $6, now(), '{}'::jsonb)
      RETURNING *
      `,
      [crypto.randomUUID(), lobbyId, tableId, fromPeer, toPeer, FlowState.ANNOUNCED]
    );
    this.logger.info("flow.announce", { flowId: flow.id, fromPeer, toPeer });
    return flow;
  }

  async claimMstCandidateFlows(localPeers) {
    if (localPeers.length === 0) return [];
    const { rows } = await this.query(
      `
      WITH candidates AS (
        SELECT flow.id
        FROM private_mesh.peer_connection_flows flow
        WHERE (
            flow.mst_state = $3
            OR (
              flow.mst_state = $4
              AND flow.mst_processing_started_at < now() - ($8::int * interval '1 millisecond')
            )
          )
          AND flow.from_peer = ANY($1::text[])
          AND NOT EXISTS (
            SELECT 1
            FROM private_mesh.peer_connection_flows reverse_flow
            WHERE reverse_flow.lobby_id = flow.lobby_id
              AND reverse_flow.table_id = flow.table_id
              AND reverse_flow.from_peer = flow.to_peer
              AND reverse_flow.to_peer = flow.from_peer
              AND reverse_flow.created_at < flow.created_at
              AND reverse_flow.mst_state NOT IN ($5, $6, $7)
          )
        ORDER BY flow.lobby_id, flow.table_id, flow.created_at, flow.id
      )
      UPDATE private_mesh.peer_connection_flows flow
      SET
        mst_state = $4,
        mst_processor = $2,
        mst_processing_started_at = now(),
        last_update_at = now()
      FROM candidates
      WHERE flow.id = candidates.id
      RETURNING flow.*
      `,
      [
        localPeers,
        this.config.nodeId,
        FlowState.ANNOUNCED,
        FlowState.PROCESSING,
        FlowState.CONNECTED,
        FlowState.CONNECTED_P2P,
        FlowState.FAILED,
        this.config.processingTimeoutMs,
      ]
    );
    return rows;
  }

  async setMstResult({ selectedIds, offloadedIds }) {
    const changed = [];
    if (selectedIds.length > 0) {
      const { rows } = await this.query(
        `
        UPDATE private_mesh.peer_connection_flows
        SET
          mst_state = $2,
          mst_processor = $3,
          last_update_at = now()
        WHERE id = ANY($1::text[])
          AND mst_state = $4
          AND mst_processor = $3
        RETURNING *
        `,
        [selectedIds, FlowState.REQUEST_OFFER, this.config.nodeId, FlowState.PROCESSING]
      );
      changed.push(...rows);
    }

    if (offloadedIds.length > 0) {
      const { rows } = await this.query(
        `
        UPDATE private_mesh.peer_connection_flows
        SET
          mst_state = $2,
          mst_processor = $3,
          last_update_at = now()
        WHERE id = ANY($1::text[])
          AND mst_state = $4
          AND mst_processor = $3
        RETURNING *
        `,
        [offloadedIds, FlowState.AWAITING_P2P, this.config.nodeId, FlowState.PROCESSING]
      );
      changed.push(...rows);
    }
    return changed;
  }

  async storeOffer(signal) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = $3::jsonb,
        mst_state = $4,
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $5
          AND to_peer = $6
          AND mst_state IN ($7, $8, $9)
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [
        signal.lobbyId,
        signal.tableId,
        signalPayload(signal),
        FlowState.OFFER,
        signal.from,
        signal.to,
        FlowState.REQUEST_OFFER,
        FlowState.PROCESSING,
        FlowState.AWAITING_P2P,
      ]
    );
  }

  async storeAnswer(signal) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = $3::jsonb,
        mst_state = $4,
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $5
          AND to_peer = $6
          AND mst_state = $7
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [
        signal.lobbyId,
        signal.tableId,
        signalPayload(signal),
        FlowState.ANSWER,
        signal.to,
        signal.from,
        FlowState.OFFER,
      ]
    );
  }

  async markConnected({ lobbyId, tableId, fromPeer, toPeer }) {
    const connected = await this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
        mst_state = $5,
        connected_at = now(),
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $3
          AND to_peer = $4
          AND mst_state IN ($6, $7)
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [lobbyId, tableId, fromPeer, toPeer, FlowState.CONNECTED, FlowState.ANSWER, FlowState.OFFER]
    );

    if (connected) {
      await this.requestReverseP2p({ lobbyId, tableId, fromPeer: toPeer, toPeer: fromPeer });
      return connected;
    }

    return this.markConnectedP2p({ lobbyId, tableId, fromPeer, toPeer });
  }

  async requestReverseP2p({ lobbyId, tableId, fromPeer, toPeer }) {
    const terminal = await this.oneOrNull(
      `
      SELECT *
      FROM private_mesh.peer_connection_flows
      WHERE lobby_id = $1
        AND table_id = $2
        AND from_peer = $3
        AND to_peer = $4
        AND mst_state IN ($5, $6)
      ORDER BY last_update_at DESC
      LIMIT 1
      `,
      [lobbyId, tableId, fromPeer, toPeer, FlowState.CONNECTED, FlowState.CONNECTED_P2P]
    );
    if (terminal) return terminal;

    const existing = await this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
        mst_state = $5,
        mst_processor = $6,
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $3
          AND to_peer = $4
          AND mst_state NOT IN ($7, $8)
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [
        lobbyId,
        tableId,
        fromPeer,
        toPeer,
        FlowState.CONNECT_P2P,
        this.config.nodeId,
        FlowState.CONNECTED,
        FlowState.CONNECTED_P2P,
      ]
    );
    if (existing) return existing;

    return this.one(
      `
      INSERT INTO private_mesh.peer_connection_flows (
        id, lobby_id, table_id, from_peer, to_peer, data, mst_state, mst_processor, last_update_at
      )
      VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, now())
      ON CONFLICT (id) DO NOTHING
      RETURNING *
      `,
      [crypto.randomUUID(), lobbyId, tableId, fromPeer, toPeer, FlowState.CONNECT_P2P, this.config.nodeId]
    );
  }

  async markConnectedP2p({ lobbyId, tableId, fromPeer, toPeer }) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
        mst_state = $5,
        connected_at = now(),
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $3
          AND to_peer = $4
          AND mst_state = $6
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [lobbyId, tableId, fromPeer, toPeer, FlowState.CONNECTED_P2P, FlowState.CONNECT_P2P]
    );
  }

  async markFailed({ lobbyId, tableId, fromPeer, toPeer, reason }) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        mst_state = $5,
        last_error = $6,
        last_update_at = now()
      WHERE id = (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE lobby_id = $1
          AND table_id = $2
          AND from_peer = $3
          AND to_peer = $4
        ORDER BY last_update_at DESC
        LIMIT 1
      )
      RETURNING *
      `,
      [lobbyId, tableId, fromPeer, toPeer, FlowState.FAILED, reason]
    );
  }

  async actionableFlows(localPeers) {
    if (localPeers.length === 0) return [];
    const { rows } = await this.query(
      `
      SELECT *
      FROM private_mesh.peer_connection_flows
      WHERE (
          mst_state = $2
          AND from_peer = ANY($1::text[])
        )
        OR (
          mst_state = $3
          AND to_peer = ANY($1::text[])
        )
        OR (
          mst_state = $4
          AND from_peer = ANY($1::text[])
        )
        OR (
          mst_state = $5
          AND from_peer = ANY($1::text[])
        )
      ORDER BY last_update_at, id
      `,
      [
        localPeers,
        FlowState.REQUEST_OFFER,
        FlowState.OFFER,
        FlowState.ANSWER,
        FlowState.CONNECT_P2P,
      ]
    );
    return rows;
  }

  async flowCounts() {
    const { rows } = await this.query(
      `
      SELECT mst_state, count(*)::int AS count
      FROM private_mesh.peer_connection_flows
      GROUP BY mst_state
      `
    );
    return Object.fromEntries(rows.map((row) => [row.mst_state, row.count]));
  }

  async query(sql, params = []) {
    if (!this.pool) this.start();
    return this.pool.query(sql, params);
  }

  async one(sql, params = []) {
    const { rows } = await this.query(sql, params);
    if (!rows[0]) throw new Error("expected database row was not returned");
    return rows[0];
  }

  async oneOrNull(sql, params = []) {
    const { rows } = await this.query(sql, params);
    return rows[0] ?? null;
  }
}

function signalPayload(signal) {
  return {
    signal: {
      id: signal.id,
      originAddress: signal.from,
      targetAddress: signal.to,
      sender: signal.from,
      recipient: signal.to,
      encryptedData: signal.encryptedData,
      payload: signal.encryptedData,
      kind: signal.kind,
      lobbyId: signal.lobbyId,
      tableId: signal.tableId,
      sent_at: signal.sentAt,
    },
  };
}
