import { validateFeedId } from "../store/feed-repository.js";

const MAX_ROUNDS = 25;
const MAX_RESYNCS = 5;
const PAGE_LIMIT = 500;

// Pull loop for JSON-kind feeds. Each round reads the durable cursor fresh
// from the database, so increments pushed through ingestEvents while a sync
// (or its alignment fetch) is in flight are never overwritten by it. When the
// source demands a full re-alignment, the catalog is fetched first and
// committed with version guards, then the loop resumes from whatever cursor
// is current after the alignment — never from a stale local copy.
export class SyncService {
  constructor({ feeds, jsonFeeds, syncs, sourceClient = null, clock = () => new Date() }) {
    this.feeds = feeds;
    this.jsonFeeds = jsonFeeds;
    this.syncs = syncs;
    this.sourceClient = sourceClient;
    this.clock = clock;
    this.inFlight = new Map();
  }

  sync(feedId) {
    const active = this.inFlight.get(feedId);
    if (active) return active;
    const task = this.perform(feedId).finally(() => {
      if (this.inFlight.get(feedId) === task) this.inFlight.delete(feedId);
    });
    this.inFlight.set(feedId, task);
    return task;
  }

  async perform(feedId) {
    validateFeedId(feedId);
    const feed = this.feeds.get(feedId);
    if (!feed) throw Object.assign(new Error(`unknown feed: ${feedId}`), { statusCode: 404 });
    if (feed.kind !== "json") {
      throw Object.assign(new Error(`feed ${feedId} is an HTML feed; use refresh instead of sync`), { statusCode: 409 });
    }
    if (!this.sourceClient) {
      throw Object.assign(new Error("no JSON source client is configured for this archive"), { statusCode: 503 });
    }
    const totals = { applied: 0, replay: 0, stale: 0, conflict: 0, invalid: 0, unknownKey: 0 };
    let batches = 0;
    let resyncs = 0;
    const startedAt = this.clock().toISOString();
    try {
      for (let round = 0; round < MAX_ROUNDS; round += 1) {
        const current = this.jsonFeeds.cursor(feedId);
        const response = await this.sourceClient.fetchEvents({
          feed,
          cursor: current?.cursor ?? null,
          limit: PAGE_LIMIT
        });
        if (response?.resyncRequired) {
          resyncs += 1;
          if (resyncs > MAX_RESYNCS) throw new Error("source demanded full re-alignment too many times");
          const catalog = await this.sourceClient.fetchCatalog({ feed });
          this.jsonFeeds.alignCatalog(feedId, catalog);
          continue;
        }
        const events = response?.events ?? [];
        if (!events.length) break;
        const result = this.jsonFeeds.ingestEvents(feedId, events);
        batches += 1;
        for (const key of Object.keys(totals)) totals[key] += result.counts[key] ?? 0;
        // Caught up, or the source keeps redelivering what we already stored.
        if (result.counts.applied + result.counts.stale + result.counts.invalid
          + result.counts.conflict + result.counts.unknownKey === 0) break;
      }
      return {
        feedId,
        outcome: "completed",
        batches,
        resyncs,
        counts: totals,
        cursor: this.jsonFeeds.cursor(feedId)
      };
    } catch (error) {
      // ingestEvents/alignCatalog already logged their own failed run.
      if (!error.syncRecorded) {
        this.syncs.record({
          feedId, kind: "events", startedAt, finishedAt: this.clock().toISOString(),
          outcome: "failed", error: error.message.slice(0, 500)
        });
      }
      throw Object.assign(error, { statusCode: error.statusCode ?? 502 });
    }
  }

  pending(feedId) {
    return this.inFlight.has(feedId);
  }
}
