export class RefreshService {
  constructor({ feeds, fetches, fetcher, imports, clock = () => new Date() }) {
    this.feeds = feeds;
    this.fetches = fetches;
    this.fetcher = fetcher;
    this.imports = imports;
    this.clock = clock;
    this.inFlight = new Map();
  }

  refresh(feedId) {
    const active = this.inFlight.get(feedId);
    if (active) return active;
    const task = this.perform(feedId).finally(() => {
      if (this.inFlight.get(feedId) === task) this.inFlight.delete(feedId);
    });
    this.inFlight.set(feedId, task);
    return task;
  }

  async perform(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    if (feed.kind !== "html") {
      throw Object.assign(new Error(`feed ${feedId} is a JSON feed; use sync instead of refresh`), { statusCode: 409 });
    }
    const startedAt = this.clock().toISOString();
    try {
      const fetched = await this.fetcher.fetchFeed(feed);
      const finishedAt = this.clock().toISOString();
      if (fetched.status === 304) {
        const attemptId = this.fetches.record({
          feedId, startedAt, finishedAt, outcome: "not_modified", httpStatus: 304,
          attemptCount: fetched.attemptCount, requestedUrl: fetched.requestedUrl,
          finalUrl: fetched.finalUrl, captureId: null, errorCode: null, errorMessage: null
        });
        return { attemptId, feedId, outcome: "not_modified", httpStatus: 304, capture: null };
      }
      const capture = this.imports.importHtml(feedId, fetched.html, finishedAt);
      const attemptId = this.fetches.record({
        feedId, startedAt, finishedAt, outcome: capture.accepted ? "accepted" : "rejected",
        httpStatus: fetched.status, attemptCount: fetched.attemptCount,
        requestedUrl: fetched.requestedUrl, finalUrl: fetched.finalUrl,
        captureId: capture.captureId ?? null, errorCode: capture.accepted ? null : "capture_rejected",
        errorMessage: capture.reason
      });
      return { attemptId, feedId, outcome: capture.accepted ? "accepted" : "rejected",
        httpStatus: fetched.status, capture };
    } catch (error) {
      const finishedAt = this.clock().toISOString();
      const attemptId = this.fetches.record({
        feedId, startedAt, finishedAt, outcome: "failed", httpStatus: error.status ?? null,
        attemptCount: error.attemptCount ?? 1, requestedUrl: feed.baseUrl, finalUrl: null,
        captureId: null, errorCode: error.code ?? "refresh_failed", errorMessage: error.message.slice(0, 500)
      });
      throw Object.assign(error, { attemptId, statusCode: error.statusCode ?? 502 });
    }
  }

  recent(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.fetches.recent(feedId, limit);
  }

  pending(feedId) {
    return this.inFlight.has(feedId);
  }
}
