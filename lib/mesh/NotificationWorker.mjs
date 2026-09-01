export class NotificationWorker {
  constructor({ repository, onWake, logger }) {
    this.repository = repository;
    this.onWake = onWake;
    this.logger = logger;
    this.client = null;
    this.stopped = true;
    this.reconnectTimer = null;
  }

  start() {
    if (!this.stopped) return;
    this.stopped = false;
    void this.connect();
  }

  async stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    if (this.client) await this.client.end().catch(() => {});
    this.client = null;
  }

  async connect() {
    if (this.stopped) return;
    try {
      const client = this.repository.createListener();
      client.on("notification", (message) => this.handleNotification(message));
      client.on("error", (error) => this.reconnect(error));
      client.on("end", () => this.reconnect());
      await client.connect();
      await client.query("LISTEN private_mesh_events");
      this.client = client;
      this.logger.info("postgres.listen", { channel: "private_mesh_events" });
    } catch (error) {
      this.reconnect(error);
    }
  }

  handleNotification(message) {
    let payload = {};
    try {
      payload = message.payload ? JSON.parse(message.payload) : {};
    } catch {
      payload = { raw: message.payload };
    }
    this.logger.info("postgres.notify", { type: payload.type });
    this.onWake?.(payload);
  }

  reconnect(error) {
    if (this.stopped) return;
    if (error) this.logger.error("postgres.listen.failed", error);
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, 1_000);
  }
}
