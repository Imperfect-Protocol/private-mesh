export class HeartbeatWorker {
  constructor({ repository, config, logger }) {
    this.repository = repository;
    this.config = config;
    this.logger = logger;
    this.timer = null;
  }

  async start() {
    await this.repository.registerMeshNode();
    this.timer ??= setInterval(() => {
      this.repository.heartbeatMeshNode()
        .catch((error) => this.logger.error("mesh.heartbeat.failed", error));
    }, this.config.heartbeatIntervalMs);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}
