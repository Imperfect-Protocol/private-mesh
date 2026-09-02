import { FlowState } from "./flow-states.mjs";

export class FlowReactionWorker {
  constructor({ repository, peerRegistry, outbox, logger }) {
    this.repository = repository;
    this.peerRegistry = peerRegistry;
    this.outbox = outbox;
    this.logger = logger;
    this.delivered = new Set();
    this.running = false;
  }

  async runOnce() {
    if (this.running) return;
    this.running = true;
    try {
      const localPeers = this.peerRegistry.activeAddresses();
      const flows = await this.repository.actionableFlows(localPeers);
      for (const flow of flows) this.handleFlow(flow);
    } finally {
      this.running = false;
    }
  }

  handleFlow(flow) {
    if (flow.mst_state === FlowState.REQUEST_OFFER) {
      this.deliverOnce(flow, FlowState.REQUEST_OFFER, flow.from_peer, {
        type: "trigger-offer",
        flowId: flow.id,
        target: flow.to_peer,
        lobbyId: flow.lobby_id,
        tableId: flow.table_id,
      });
      return;
    }

    if (flow.mst_state === FlowState.OFFER) {
      this.deliverOnce(flow, FlowState.OFFER, flow.to_peer, {
        type: "bootstrap-offer",
        flowId: flow.id,
        peer: flow.from_peer,
        signal: flow.data?.signal,
      }, this.clearDataAfterDelivery(flow, FlowState.OFFER));
      return;
    }

    if (flow.mst_state === FlowState.ANSWER) {
      this.deliverOnce(flow, FlowState.ANSWER, flow.from_peer, {
        type: "bootstrap-answer",
        flowId: flow.id,
        peer: flow.to_peer,
        signal: flow.data?.signal,
      }, this.clearDataAfterDelivery(flow, FlowState.ANSWER));
      return;
    }

    if (flow.mst_state === FlowState.CONNECT_P2P) {
      this.deliverOnce(flow, FlowState.CONNECT_P2P, flow.from_peer, {
        type: "p2p-route-available",
        flowId: flow.id,
        target: flow.to_peer,
        status: "connect_through_peers",
        lobbyId: flow.lobby_id,
        tableId: flow.table_id,
      });
    }
  }

  deliverOnce(flow, state, recipient, message, onDelivered) {
    const key = `${flow.id}:${state}:${flow.last_update_at}`;
    if (this.delivered.has(key)) return;
    this.delivered.add(key);
    this.outbox.enqueue({
      lobbyId: flow.lobby_id,
      tableId: flow.table_id,
      recipient,
      message,
      onDelivered,
    });
    this.pruneDelivered();
    this.logger.info("flow.deliver", { flowId: flow.id, state, recipient });
  }

  clearDataAfterDelivery(flow, state) {
    return () => {
      this.repository.clearFlowData(flow.id, state)
        .catch((error) => this.logger.error("flow.data.clear.failed", error, { flowId: flow.id, state }));
    };
  }

  pruneDelivered() {
    if (this.delivered.size <= 5_000) return;
    this.delivered = new Set([...this.delivered].slice(-2_500));
  }
}
