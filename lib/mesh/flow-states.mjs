export const FlowState = Object.freeze({
  ANNOUNCED: "Announced",
  PROCESSING: "Processing",
  REQUEST_OFFER: "RequestOffer",
  AWAITING_P2P: "AwaitingP2P",
  OFFER: "Offer",
  ANSWER: "Answer",
  CONNECTED: "Connected",
  CONNECT_P2P: "ConnectP2P",
  CONNECTED_P2P: "ConnectedP2P",
  FAILED: "Failed",
});

export const terminalStates = new Set([
  FlowState.CONNECTED,
  FlowState.CONNECTED_P2P,
  FlowState.FAILED,
]);
