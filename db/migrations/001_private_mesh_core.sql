CREATE SCHEMA IF NOT EXISTS private_mesh;

CREATE OR REPLACE FUNCTION private_mesh.notify_mesh_event(
  event_type text,
  event_payload jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_notify(
    'private_mesh_events',
    jsonb_build_object(
      'type', event_type,
      'payload', coalesce(event_payload, '{}'::jsonb),
      'notifiedAt', now()
    )::text
  );
END;
$$;

CREATE OR REPLACE FUNCTION private_mesh.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION private_mesh.notify_peer_connection_flow_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM private_mesh.notify_mesh_event(
    'peer-flow-changed',
    jsonb_build_object(
      'flowId', NEW.id,
      'lobbyId', NEW.lobby_id,
      'tableId', NEW.table_id,
      'fromPeer', NEW.from_peer,
      'toPeer', NEW.to_peer,
      'mstState', NEW.mst_state,
      'mstProcessor', NEW.mst_processor,
      'lastUpdateAt', NEW.last_update_at
    )
  );
  RETURN NEW;
END;
$$;
