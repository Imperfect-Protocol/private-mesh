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
      this.pool = new Pool(this.connectionOptions());
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
    return new Client(this.connectionOptions());
  }

  connectionOptions() {
    const useSsl = this.config.postgres.ssl === true;
    return {
      connectionString: useSsl ? withoutSslMode(this.config.databaseUrl) : this.config.databaseUrl,
      ssl: useSsl ? { rejectUnauthorized: false } : undefined,
    };
  }

  async insertAnnounce({ lobbyId, tableId, fromPeer, toPeer }) {
    if (!this.pool) this.start();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await this.lockFlowPair(client, { lobbyId, tableId, fromPeer, toPeer });

      const { rows } = await client.query(
        `
        INSERT INTO private_mesh.peer_connection_flows (
          id, lobby_id, table_id, from_peer, to_peer, data, mst_state, last_update_at, metadata
        )
        VALUES ($1, $2, $3, $4, $5, NULL, $6, now(), '{}'::jsonb)
        RETURNING *
        `,
        [crypto.randomUUID(), lobbyId, tableId, fromPeer, toPeer, FlowState.ANNOUNCED]
      );
      await this.failSupersededSameDirectionFlows(client, {
        lobbyId,
        tableId,
        fromPeer,
        toPeer,
        currentFlowId: rows[0].id,
      });
      await client.query("COMMIT");
      this.logger.info("flow.announce", { flowId: rows[0].id, fromPeer, toPeer, state: rows[0].mst_state });
      return rows[0];
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async expireStaleFlows() {
    const { rows } = await this.query(
      `
      WITH expired AS (
        SELECT id
        FROM private_mesh.peer_connection_flows
        WHERE mst_state NOT IN ($2, $4, $5, $6)
          AND last_update_at < now() - ($1::int * interval '1 millisecond')
        ORDER BY last_update_at, id
        FOR UPDATE SKIP LOCKED
      )
      UPDATE private_mesh.peer_connection_flows flow
      SET
        data = NULL,
        mst_state = $2,
        last_error = $3,
        last_update_at = now()
      FROM expired
      WHERE flow.id = expired.id
      RETURNING flow.*
      `,
      [
        this.config.messageTtlMs,
        FlowState.EXPIRED,
        "expired",
        FlowState.FAILED,
        FlowState.CONNECTED,
        FlowState.CONNECTED_P2P,
      ]
    );
    return rows;
  }

  async applyMstPlan(localPeers, buildPlan) {
    if (localPeers.length === 0) {
      return { changed: [], conflicted: false, candidateCount: 0 };
    }

    if (!this.pool) this.start();
    const client = await this.pool.connect();
    let candidateCount = 0;
    try {
      await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE");

      const candidates = await this.loadMstPlanningFlows(client, localPeers);
      candidateCount = candidates.length;
      if (candidates.length === 0) {
        await client.query("COMMIT");
        return { changed: [], conflicted: false, candidateCount };
      }

      const plan = normalizeMstPlan(candidates, buildPlan(candidates));
      await this.lockMstPlanRows(client, plan);
      const changed = await this.writeMstPlan(client, plan);

      await client.query("COMMIT");
      return { changed, conflicted: false, candidateCount };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      if (isMstConflict(error)) {
        return { changed: [], conflicted: true, candidateCount };
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async loadMstPlanningFlows(client, localPeers) {
    const { rows } = await client.query(
      `
      SELECT flow.*
      FROM private_mesh.peer_connection_flows flow
      WHERE flow.mst_state = ANY($2::text[])
        AND flow.last_update_at >= now() - ($3::int * interval '1 millisecond')
        AND (
          flow.from_peer = ANY($1::text[])
          OR flow.to_peer = ANY($1::text[])
        )
        AND NOT EXISTS (
          SELECT 1
          FROM private_mesh.peer_connection_flows fresh
          WHERE fresh.lobby_id = flow.lobby_id
            AND fresh.table_id = flow.table_id
            AND least(fresh.from_peer, fresh.to_peer) = least(flow.from_peer, flow.to_peer)
            AND greatest(fresh.from_peer, fresh.to_peer) = greatest(flow.from_peer, flow.to_peer)
            AND fresh.mst_state = $4
            AND fresh.created_at >= now() - ($5::int * interval '1 millisecond')
        )
      ORDER BY flow.lobby_id, flow.table_id, flow.created_at DESC, flow.id DESC
      `,
      [
        localPeers,
        activePrimaryStates(),
        this.config.messageTtlMs,
        FlowState.ANNOUNCED,
        this.config.announceSettleMs,
      ]
    );
    return rows;
  }

  async lockMstPlanRows(client, plan) {
    if (plan.mutableIds.length === 0) return;
    await client.query(
      `
      SELECT id
      FROM private_mesh.peer_connection_flows
      WHERE id = ANY($1::text[])
      ORDER BY id
      FOR UPDATE NOWAIT
      `,
      [plan.mutableIds]
    );
  }

  async writeMstPlan(client, plan) {
    const changed = [];

    if (plan.selectedMutableIds.length > 0) {
      const { rows } = await client.query(
        `
        UPDATE private_mesh.peer_connection_flows flow
        SET
          mst_state = $2,
          mst_processor = $3,
          mst_processing_started_at = NULL,
          last_error = NULL,
          last_update_at = now()
        WHERE flow.id = ANY($1::text[])
          AND flow.mst_state = ANY($4::text[])
        RETURNING flow.*
        `,
        [
          plan.selectedMutableIds,
          FlowState.REQUEST_OFFER,
          this.config.nodeId,
          mutableMstInputStates(),
        ]
      );
      if (rows.length !== plan.selectedMutableIds.length) throw new MstConflictError();
      changed.push(...rows);
    }

    if (plan.offloadedMutableIds.length > 0) {
      const { rows } = await client.query(
        `
        WITH selected_flows AS (
          SELECT *
          FROM private_mesh.peer_connection_flows
          WHERE id = ANY($5::text[])
        ),
        offload_targets AS (
          SELECT
            offloaded_flow.id AS reverse_id,
            primary_flow.id AS primary_id
          FROM private_mesh.peer_connection_flows offloaded_flow
          LEFT JOIN LATERAL (
            SELECT selected_flow.id
            FROM selected_flows selected_flow
            WHERE selected_flow.lobby_id = offloaded_flow.lobby_id
              AND selected_flow.table_id = offloaded_flow.table_id
              AND selected_flow.from_peer = offloaded_flow.to_peer
              AND selected_flow.to_peer = offloaded_flow.from_peer
            ORDER BY selected_flow.created_at DESC, selected_flow.id DESC
            LIMIT 1
          ) primary_flow ON true
          WHERE offloaded_flow.id = ANY($1::text[])
        )
        UPDATE private_mesh.peer_connection_flows flow
        SET
          data = NULL,
          mst_state = $2,
          mst_processor = $3,
          mst_processing_started_at = NULL,
          last_error = NULL,
          last_update_at = now(),
          metadata = CASE
            WHEN offload_targets.primary_id IS NULL THEN flow.metadata
            ELSE jsonb_set(
              jsonb_set(
                flow.metadata,
                '{awaitingPrimaryFlowId}',
                to_jsonb(offload_targets.primary_id),
                true
              ),
              '{awaitingReason}',
              to_jsonb($6::text),
              true
            )
          END
        FROM offload_targets
        WHERE flow.id = offload_targets.reverse_id
          AND flow.mst_state = ANY($4::text[])
          AND offload_targets.primary_id IS NOT NULL
        RETURNING flow.*
        `,
        [
          plan.offloadedMutableIds,
          FlowState.AWAITING_P2P,
          this.config.nodeId,
          activePrimaryStates(),
          plan.selectedIds,
          "opposite_direction_not_in_mst",
        ]
      );
      if (rows.length !== plan.offloadedMutableIds.length) throw new MstConflictError();
      changed.push(...rows);
    }

    return changed;
  }

  async connectAwaitingP2pEdges(localPeers) {
    if (localPeers.length === 0) return [];
    const { rows } = await this.query(
      `
      UPDATE private_mesh.peer_connection_flows reverse_flow
      SET
        data = NULL,
        mst_state = $2,
        mst_processor = $3,
        last_error = NULL,
        last_update_at = now()
      WHERE reverse_flow.from_peer = ANY($1::text[])
        AND reverse_flow.mst_state = $4
        AND EXISTS (
          SELECT 1
          FROM private_mesh.peer_connection_flows primary_flow
          WHERE primary_flow.lobby_id = reverse_flow.lobby_id
            AND primary_flow.table_id = reverse_flow.table_id
            AND primary_flow.from_peer = reverse_flow.to_peer
            AND primary_flow.to_peer = reverse_flow.from_peer
            AND primary_flow.mst_state = $5
            AND reverse_flow.metadata->>'awaitingPrimaryFlowId' = primary_flow.id
        )
      RETURNING reverse_flow.*
      `,
      [
        localPeers,
        FlowState.CONNECT_P2P,
        this.config.nodeId,
        FlowState.AWAITING_P2P,
        FlowState.CONNECTED,
      ]
    );
    return rows;
  }

  async failSupersededSameDirectionFlows(client, { lobbyId, tableId, fromPeer, toPeer, currentFlowId }) {
    await client.query(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
        mst_state = $5,
        last_error = $6,
        last_update_at = now()
      WHERE lobby_id = $1
        AND table_id = $2
        AND from_peer = $3
        AND to_peer = $4
        AND id <> $10
        AND mst_state NOT IN ($5, $7, $8, $9)
      `,
      [
        lobbyId,
        tableId,
        fromPeer,
        toPeer,
        FlowState.FAILED,
        "superseded_by_new_announce",
        FlowState.CONNECTED,
        FlowState.CONNECTED_P2P,
        FlowState.EXPIRED,
        currentFlowId,
      ]
    );
  }

  async lockFlowPair(client, { lobbyId, tableId, fromPeer, toPeer }) {
    await client.query(
      `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
      [flowPairLockKey({ lobbyId, tableId, fromPeer, toPeer })]
    );
  }

  async storeOffer(signal) {
    if (!signal.flowId) return null;
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = $3::jsonb,
        mst_state = $4,
        last_error = NULL,
        last_update_at = now()
      WHERE id = $8
        AND lobby_id = $1
        AND table_id = $2
        AND from_peer = $5
        AND to_peer = $6
        AND mst_state = $7
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
        signal.flowId,
      ]
    );
  }

  async storeAnswer(signal) {
    if (!signal.flowId) return null;
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = $3::jsonb,
        mst_state = $4,
        last_error = NULL,
        last_update_at = now()
      WHERE id = $8
        AND lobby_id = $1
        AND table_id = $2
        AND from_peer = $5
        AND to_peer = $6
        AND mst_state = $7
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
        signal.flowId,
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
        last_error = NULL,
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

    if (connected) return connected;
    return this.markConnectedP2p({ lobbyId, tableId, fromPeer, toPeer });
  }

  async markConnectedP2p({ lobbyId, tableId, fromPeer, toPeer }) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
        mst_state = $5,
        last_error = NULL,
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

  async clearFlowData(flowId, state) {
    await this.query(
      `
      UPDATE private_mesh.peer_connection_flows
      SET data = NULL
      WHERE id = $1
        AND mst_state = $2
        AND data IS NOT NULL
      `,
      [flowId, state]
    );
  }

  async markFailed({ lobbyId, tableId, fromPeer, toPeer, reason }) {
    return this.oneOrNull(
      `
      UPDATE private_mesh.peer_connection_flows
      SET
        data = NULL,
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
          AND data IS NOT NULL
        )
        OR (
          mst_state = $4
          AND from_peer = ANY($1::text[])
          AND data IS NOT NULL
        )
        OR (
          mst_state = $5
          AND from_peer = ANY($1::text[])
        )
      ORDER BY last_update_at DESC, id DESC
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
      flowId: signal.flowId,
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

function activePrimaryStates() {
  return [
    FlowState.ANNOUNCED,
    FlowState.PROCESSING,
    FlowState.REQUEST_OFFER,
    FlowState.OFFER,
    FlowState.ANSWER,
  ];
}

function mutableMstInputStates() {
  return [
    FlowState.ANNOUNCED,
    FlowState.PROCESSING,
  ];
}

function normalizeMstPlan(candidates, plan) {
  const selectedIds = unique(plan?.selectedIds ?? []);
  const offloadedIds = unique(plan?.offloadedIds ?? []);
  const selected = new Set(selectedIds);
  const offloaded = new Set(offloadedIds);

  return {
    selectedIds,
    offloadedIds,
    mutableIds: candidates
      .filter((flow) => selected.has(flow.id) || offloaded.has(flow.id))
      .filter((flow) => activePrimaryStates().includes(flow.mst_state))
      .map((flow) => flow.id),
    selectedMutableIds: candidates
      .filter((flow) => selected.has(flow.id) && mutableMstInputStates().includes(flow.mst_state))
      .map((flow) => flow.id),
    offloadedMutableIds: candidates
      .filter((flow) => offloaded.has(flow.id) && activePrimaryStates().includes(flow.mst_state))
      .map((flow) => flow.id),
  };
}

function unique(values) {
  return [...new Set(values)];
}

class MstConflictError extends Error {
  constructor() {
    super("MST transaction conflicted with a newer database state");
    this.code = "MST_CONFLICT";
  }
}

function isMstConflict(error) {
  return (
    error?.code === "MST_CONFLICT" ||
    error?.code === "40001" ||
    error?.code === "40P01" ||
    error?.code === "55P03"
  );
}

function flowPairLockKey({ lobbyId, tableId, fromPeer, toPeer }) {
  const [left, right] = [fromPeer, toPeer].sort();
  return `${lobbyId}:${tableId}:${left}:${right}`;
}

function withoutSslMode(value) {
  const url = new URL(value);
  url.searchParams.delete("sslmode");
  return url.toString();
}
