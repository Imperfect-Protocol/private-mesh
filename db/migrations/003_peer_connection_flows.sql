CREATE TABLE IF NOT EXISTS private_mesh.peer_connection_flows (
  id text PRIMARY KEY,
  lobby_id text NOT NULL,
  table_id text NOT NULL,
  from_peer text NOT NULL,
  to_peer text NOT NULL,
  data jsonb,
  mst_state text NOT NULL DEFAULT 'Announced',
  mst_processor text REFERENCES private_mesh.mesh_nodes(address) ON DELETE SET NULL,
  mst_processing_started_at timestamptz,
  last_update_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  connected_at timestamptz,
  last_error text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

  CONSTRAINT peer_connection_flows_addresses_hex
    CHECK (
      from_peer ~ '^0x[0-9a-f]{40}$'
      AND to_peer ~ '^0x[0-9a-f]{40}$'
      AND from_peer <> to_peer
    ),
  CONSTRAINT peer_connection_flows_mst_state_known
    CHECK (mst_state IN (
      'Announced',
      'Processing',
      'RequestOffer',
      'AwaitingP2P',
      'Offer',
      'Answer',
      'Connected',
      'ConnectP2P',
      'ConnectedP2P',
      'Failed'
    ))
);

CREATE INDEX IF NOT EXISTS peer_connection_flows_from_idx
  ON private_mesh.peer_connection_flows (lobby_id, table_id, from_peer, mst_state, last_update_at DESC);

CREATE INDEX IF NOT EXISTS peer_connection_flows_to_idx
  ON private_mesh.peer_connection_flows (lobby_id, table_id, to_peer, mst_state, last_update_at DESC);

CREATE INDEX IF NOT EXISTS peer_connection_flows_mst_idx
  ON private_mesh.peer_connection_flows (mst_state, mst_processing_started_at, created_at);

DROP TRIGGER IF EXISTS peer_connection_flows_touch_updated_at
  ON private_mesh.peer_connection_flows;

CREATE TRIGGER peer_connection_flows_touch_updated_at
BEFORE UPDATE ON private_mesh.peer_connection_flows
FOR EACH ROW
EXECUTE FUNCTION private_mesh.touch_updated_at();

DROP TRIGGER IF EXISTS peer_connection_flows_notify_changed
  ON private_mesh.peer_connection_flows;

CREATE TRIGGER peer_connection_flows_notify_changed
AFTER INSERT OR UPDATE OF data, mst_state, mst_processor, mst_processing_started_at, connected_at, last_error
ON private_mesh.peer_connection_flows
FOR EACH ROW
EXECUTE FUNCTION private_mesh.notify_peer_connection_flow_changed();
