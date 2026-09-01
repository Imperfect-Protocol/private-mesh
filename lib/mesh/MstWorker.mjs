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
      const candidates = await this.repository.claimMstCandidateFlows(localPeers);
      if (candidates.length === 0) return;

      const plan = this.planner.plan(candidates);
      const changed = await this.repository.setMstResult(plan);
      if (changed.length > 0) this.logger.info("mst.plan.applied", { changed: changed.length });
    } finally {
      this.running = false;
    }
  }
}
