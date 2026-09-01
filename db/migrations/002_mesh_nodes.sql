CREATE TABLE IF NOT EXISTS private_mesh.mesh_nodes (
  address text PRIMARY KEY,
  public_key text NOT NULL,
  instance_message text,
  region text,
  version text,
  status text NOT NULL DEFAULT 'active',
  max_connections integer NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

  CONSTRAINT mesh_nodes_address_hex
    CHECK (address ~ '^0x[0-9a-f]{40}$'),
  CONSTRAINT mesh_nodes_public_key_hex
    CHECK (public_key ~ '^0x[0-9a-f]+$' AND length(public_key) % 2 = 0),
  CONSTRAINT mesh_nodes_status_known
    CHECK (status IN ('active', 'draining', 'offline')),
  CONSTRAINT mesh_nodes_max_connections_nonnegative
    CHECK (max_connections >= 0)
);

CREATE INDEX IF NOT EXISTS mesh_nodes_active_idx
  ON private_mesh.mesh_nodes (last_seen_at DESC)
  WHERE status = 'active';

CREATE OR REPLACE FUNCTION private_mesh.register_mesh_node(
  mesh_address text,
  mesh_public_key text,
  mesh_instance_message text DEFAULT NULL,
  mesh_region text DEFAULT NULL,
  mesh_version text DEFAULT NULL,
  mesh_max_connections integer DEFAULT 0,
  mesh_metadata jsonb DEFAULT '{}'::jsonb
)
RETURNS private_mesh.mesh_nodes
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_address text := lower(mesh_address);
  registered private_mesh.mesh_nodes;
BEGIN
  INSERT INTO private_mesh.mesh_nodes (
    address,
    public_key,
    instance_message,
    region,
    version,
    status,
    max_connections,
    started_at,
    last_seen_at,
    metadata
  )
  VALUES (
    normalized_address,
    lower(mesh_public_key),
    mesh_instance_message,
    mesh_region,
    mesh_version,
    'active',
    greatest(mesh_max_connections, 0),
    now(),
    now(),
    coalesce(mesh_metadata, '{}'::jsonb)
  )
  ON CONFLICT (address) DO UPDATE
  SET
    public_key = EXCLUDED.public_key,
    instance_message = EXCLUDED.instance_message,
    region = EXCLUDED.region,
    version = EXCLUDED.version,
    status = 'active',
    max_connections = EXCLUDED.max_connections,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
  RETURNING * INTO registered;

  PERFORM private_mesh.notify_mesh_event(
    'mesh-node-announced',
    jsonb_build_object(
      'meshNode', registered.address,
      'lastSeenAt', registered.last_seen_at
    )
  );

  RETURN registered;
END;
$$;

CREATE OR REPLACE FUNCTION private_mesh.heartbeat_mesh_node(mesh_address text)
RETURNS void
LANGUAGE sql
AS $$
  UPDATE private_mesh.mesh_nodes
  SET
    last_seen_at = now(),
    status = 'active'
  WHERE address = lower(mesh_address);
$$;
