// A locally simulatable JSON increment interface. It behaves like the real
// source described by operators: every event carries a stable article key, a
// version, an upsert/delete op and the next cursor; the interface may
// redeliver already-read events, replay from an old cursor after a
// disconnect, and demand a full re-alignment. Cursors are opaque to the
// archive; here they are simply the source-wide version as a string so tests
// can observe that cursor advancement matches stored versions.
export class SyntheticJsonSource {
  constructor({ clock = () => new Date() } = {}) {
    this.clock = clock;
    this.version = 0;
    this.log = [];
    this.live = new Map();
    // Fault injection knobs, all deterministic for offline tests.
    this.redeliverCount = 0;
    this.failuresRemaining = 0;
    this.resyncRequested = false;
    this.oldestRetainedVersion = 0;
  }

  publish(key, article) {
    return this.append(key, "upsert", { ...article });
  }

  retract(key) {
    return this.append(key, "delete", null);
  }

  append(key, op, article) {
    this.version += 1;
    const event = { key, version: this.version, op, article };
    this.log.push(event);
    if (op === "upsert") this.live.set(key, { version: this.version, article });
    else this.live.delete(key);
    return { ...event, cursor: String(event.version) };
  }

  async fetchEvents({ cursor = null, limit = 500 } = {}) {
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      throw Object.assign(new Error("synthetic source connection dropped"), { code: "source_unavailable" });
    }
    const position = cursor === null || cursor === undefined ? 0 : Number(cursor);
    if (this.resyncRequested || !Number.isSafeInteger(position) || position < this.oldestRetainedVersion) {
      this.resyncRequested = false;
      return { resyncRequired: true };
    }
    let events = this.log.filter((entry) => entry.version > position).slice(0, limit);
    if (this.redeliverCount > 0) {
      const redelivered = this.log.filter((entry) => entry.version <= position).slice(-this.redeliverCount);
      this.redeliverCount = 0;
      events = [...redelivered, ...events];
    }
    return { events: events.map((entry) => ({ ...entry, cursor: String(entry.version) })) };
  }

  async fetchCatalog() {
    return {
      watermarkVersion: this.version,
      nextCursor: String(this.version),
      observedAt: this.clock().toISOString(),
      items: [...this.live.entries()].map(([key, state]) => ({
        key,
        version: state.version,
        url: state.article.url,
        title: state.article.title,
        publishedAt: state.article.publishedAt ?? null
      }))
    };
  }
}
