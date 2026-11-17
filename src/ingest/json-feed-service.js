import { validateFeedId } from "../store/feed-repository.js";
import { parseEventBatch, parseCatalog, normalizePayload } from "./json-feed.js";

// Ingest semantics for JSON-kind feeds.
//
// Incremental events and full-alignment catalogs share one rule set:
//  - Every delivery is written to feed_events as source evidence exactly once;
//    redelivered (key, version, op) triples are idempotent replays.
//  - A key's stored version never regresses: same-key deliveries with a lower
//    or equal version are recorded as stale and do not touch article state.
//  - The feed cursor advances only to the highest version actually persisted
//    in the same transaction, and never backwards; replay-only batches leave
//    it untouched.
//  - Incremental batches never mark unmentioned articles missing. Only a full
//    alignment may do that, and only for keys whose stored version is covered
//    by the catalog watermark, so increments applied while an alignment was
//    in flight are protected from the older catalog.
export class JsonFeedService {
  constructor({ database, feeds, events, syncs, clock = () => new Date() }) {
    this.database = database;
    this.feeds = feeds;
    this.events = events;
    this.syncs = syncs;
    this.clock = clock;
  }

  requireJsonFeed(feedId) {
    validateFeedId(feedId);
    const feed = this.feeds.get(feedId);
    if (!feed) throw Object.assign(new Error(`unknown feed: ${feedId}`), { statusCode: 404 });
    if (feed.kind !== "json") {
      throw Object.assign(new Error(`feed ${feedId} is an HTML feed; JSON event intake is not enabled for it`), { statusCode: 409 });
    }
    return feed;
  }

  ingestEvents(feedId, input) {
    const feed = this.requireJsonFeed(feedId);
    const batch = parseEventBatch(input);
    const startedAt = this.clock().toISOString();
    try {
      const result = this.applyEventBatch(feed, batch, startedAt);
      this.syncs.record({
        feedId, kind: "events", startedAt, finishedAt: this.clock().toISOString(),
        outcome: "completed", stats: result.counts,
        cursorBefore: result.cursor.previous, cursorAfter: result.cursor.current
      });
      return result;
    } catch (error) {
      this.syncs.record({
        feedId, kind: "events", startedAt, finishedAt: this.clock().toISOString(),
        outcome: "failed", error: error.message.slice(0, 500)
      });
      error.syncRecorded = true;
      throw error;
    }
  }

  applyEventBatch(feed, batch, receivedAt) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const before = this.events.cursor(feed.feedId);
      const preBatchMaxVersion = before?.maxVersion ?? 0;
      const outcomes = [];
      const counts = { received: batch.length, applied: 0, replay: 0, stale: 0, conflict: 0, invalid: 0, unknownKey: 0 };
      let batchMaxVersion = 0;
      let batchMaxCursor = null;
      for (const event of batch) {
        const outcome = this.applyOneEvent(feed, event, preBatchMaxVersion, receivedAt);
        counts[outcome === "unknown_key" ? "unknownKey" : outcome] += 1;
        outcomes.push({ key: event.key, version: event.version, op: event.op, outcome });
        if (outcome !== "replay" && event.version > batchMaxVersion) {
          batchMaxVersion = event.version;
          batchMaxCursor = event.cursor;
        }
      }
      // The cursor may only move to a version this transaction actually
      // persisted, and must never regress behind the stored high-water mark.
      if (batchMaxVersion > preBatchMaxVersion) {
        this.events.advanceCursor(feed.feedId, batchMaxCursor, batchMaxVersion, receivedAt);
      }
      this.database.exec("COMMIT");
      const after = this.events.cursor(feed.feedId);
      return {
        feedId: feed.feedId,
        outcome: "completed",
        counts,
        events: outcomes,
        cursor: {
          previous: before?.cursor ?? null,
          current: after?.cursor ?? null,
          maxVersion: after?.maxVersion ?? 0
        }
      };
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  applyOneEvent(feed, event, preBatchMaxVersion, receivedAt) {
    const prior = this.events.deliveries(feed.feedId, event.key, event.version);
    if (prior.some((row) => row.op === event.op)) return "replay";
    const payloadJson = event.article ? JSON.stringify(event.article) : null;
    if (prior.length) {
      this.events.recordEvent({ feedId: feed.feedId, key: event.key, version: event.version, op: event.op,
        payloadJson, cursor: event.cursor, outcome: "conflict", receivedAt });
      return "conflict";
    }
    let payload = null;
    if (event.op === "upsert") {
      try {
        payload = normalizePayload(event.article ?? {}, feed);
      } catch {
        this.events.recordEvent({ feedId: feed.feedId, key: event.key, version: event.version, op: event.op,
          payloadJson, cursor: event.cursor, outcome: "invalid", receivedAt });
        return "invalid";
      }
    }
    const source = this.events.source(feed.feedId, event.key);
    let outcome;
    if (!source && event.version <= preBatchMaxVersion) {
      // Older than the position we already durably stored (typically a full
      // alignment watermark): recording only, never resurrecting old state.
      outcome = "stale";
    } else if (event.op === "delete") {
      if (!source) {
        outcome = "unknown_key";
      } else if (event.version > source.version) {
        this.events.applyDelete({ feedId: feed.feedId, key: event.key, articleId: source.articleId,
          version: event.version, now: receivedAt });
        outcome = "applied";
      } else {
        outcome = "stale";
      }
    } else if (!source) {
      this.events.createSourcedArticle({ feedId: feed.feedId, key: event.key, payload,
        version: event.version, now: receivedAt, observed: false });
      outcome = "applied";
    } else if (event.version > source.version) {
      this.events.applyUpsert({ feedId: feed.feedId, key: event.key, articleId: source.articleId,
        payload, version: event.version, now: receivedAt, observed: false });
      outcome = "applied";
    } else {
      outcome = "stale";
    }
    this.events.recordEvent({ feedId: feed.feedId, key: event.key, version: event.version, op: event.op,
      payloadJson, cursor: event.cursor, outcome, receivedAt });
    return outcome;
  }

  alignCatalog(feedId, input) {
    const feed = this.requireJsonFeed(feedId);
    const catalog = parseCatalog(input);
    const startedAt = this.clock().toISOString();
    try {
      const result = this.applyCatalog(feed, catalog, startedAt);
      this.syncs.record({
        feedId, kind: "alignment", startedAt, finishedAt: this.clock().toISOString(),
        outcome: "completed", stats: result.counts, watermarkVersion: catalog.watermarkVersion,
        cursorBefore: result.cursor.previous, cursorAfter: result.cursor.current
      });
      return result;
    } catch (error) {
      this.syncs.record({
        feedId, kind: "alignment", startedAt, finishedAt: this.clock().toISOString(),
        outcome: "failed", watermarkVersion: catalog.watermarkVersion, error: error.message.slice(0, 500)
      });
      error.syncRecorded = true;
      throw error;
    }
  }

  applyCatalog(feed, catalog, receivedAt) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const before = this.events.cursor(feed.feedId);
      const preMaxVersion = before?.maxVersion ?? 0;
      const counts = { items: catalog.items.length, applied: 0, observed: 0, stale: 0, invalid: 0, missing: 0, protected: 0 };
      const catalogKeys = new Set(catalog.items.map((item) => item.key));
      for (const item of catalog.items) {
        let payload;
        try {
          payload = normalizePayload(item.article, feed);
        } catch {
          counts.invalid += 1;
          continue;
        }
        const source = this.events.source(feed.feedId, item.key);
        if (!source) {
          this.events.createSourcedArticle({ feedId: feed.feedId, key: item.key, payload,
            version: item.version, now: receivedAt, observed: true });
          counts.applied += 1;
        } else if (item.version > source.version) {
          this.events.applyUpsert({ feedId: feed.feedId, key: item.key, articleId: source.articleId,
            payload, version: item.version, now: receivedAt, observed: true });
          counts.applied += 1;
        } else if (source.deleted) {
          // A newer delete already recorded this key's removal; the older
          // catalog listing must not resurrect it.
          counts.stale += 1;
        } else {
          this.events.applyObservation({ articleId: source.articleId, now: receivedAt });
          counts.observed += 1;
        }
      }
      // Absence from a full observation is confirmed missing — but only for
      // keys the catalog actually covers. Keys with a newer stored version
      // were touched by increments that arrived while the alignment was in
      // flight and are protected from the older catalog.
      for (const source of this.events.sources(feed.feedId)) {
        if (catalogKeys.has(source.key)) continue;
        if (source.version > catalog.watermarkVersion) {
          counts.protected += 1;
        } else if (!source.deleted) {
          this.events.applyMissing({ articleId: source.articleId, missingThreshold: feed.missingThreshold });
          counts.missing += 1;
        }
      }
      // Adopt the catalog cursor only when its watermark is at least as new
      // as everything we have stored; otherwise our cursor already points
      // past the catalog and must not regress.
      if (catalog.watermarkVersion >= preMaxVersion) {
        this.events.advanceCursor(feed.feedId, catalog.nextCursor, catalog.watermarkVersion, receivedAt);
      }
      this.database.exec("COMMIT");
      const after = this.events.cursor(feed.feedId);
      return {
        feedId: feed.feedId,
        outcome: "completed",
        counts,
        cursor: {
          previous: before?.cursor ?? null,
          current: after?.cursor ?? null,
          maxVersion: after?.maxVersion ?? 0
        }
      };
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  cursor(feedId) {
    return this.events.cursor(feedId);
  }
}
