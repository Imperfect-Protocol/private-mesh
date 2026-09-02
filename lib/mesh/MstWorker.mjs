export class MstWorker {
  constructor({ repository, peerRegistry, planner, config, logger }) {
    this.repository = repository;
    this.peerRegistry = peerRegistry;
    this.planner = planner;
    this.config = config;
    this.logger = logger;
    this.timer = null;
    this.running = false;
  }

  start() {
    this.timer ??= setInterval(() => {
      this.runOnce().catch((error) => this.logger.error("mst.run.failed", error));
    }, this.config.mstIntervalMs);
    void this.runOnce();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  async runOnce() {
    if (this.running) return;
    this.running = true;
    try {
      const localPeers = this.peerRegistry.activeAddresses();
      const mst = await this.repository.applyMstPlan(localPeers, (flows) => this.planner.plan(flows, { localPeers }));
      if (mst.conflicted) {
        this.logger.info("mst.plan.conflict", { candidates: mst.candidateCount });
      } else {
        if (mst.changed.length > 0) this.logger.info("mst.plan.applied", { changed: mst.changed.length });

        const connectedP2p = await this.repository.connectAwaitingP2pEdges(localPeers);
        if (connectedP2p.length > 0) this.logger.info("mst.reverse.connect-p2p", { changed: connectedP2p.length });
      }

      const expired = await this.repository.expireStaleFlows();
      if (expired.length > 0) this.logger.info("mst.flows.expired", { changed: expired.length });
    } finally {
      this.running = false;
    }
  }
}
