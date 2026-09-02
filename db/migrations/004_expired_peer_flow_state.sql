ALTER TABLE private_mesh.peer_connection_flows
  DROP CONSTRAINT IF EXISTS peer_connection_flows_mst_state_known;

ALTER TABLE private_mesh.peer_connection_flows
  ADD CONSTRAINT peer_connection_flows_mst_state_known
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
      'Failed',
      'Expired'
    ));
