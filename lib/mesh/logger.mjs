export class MeshLogger {
  constructor(config) {
    this.config = config;
  }

  info(action, fields = {}) {
    if (!this.config.enableLogging) return;
    console.log(new Date().toISOString(), "[mesh]", action, {
      nodeId: this.config.nodeId,
      ...fields,
    });
  }

  error(action, error, fields = {}) {
    console.error(new Date().toISOString(), "[mesh]", action, {
      nodeId: this.config.nodeId,
      message: error?.message ?? String(error),
      ...fields,
    });
  }
}
