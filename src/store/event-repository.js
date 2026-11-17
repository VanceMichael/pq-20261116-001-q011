export class EventRepository {
  constructor(database) {
    this.database = database;
    this.getCursorStmt = database.prepare(`SELECT feed_id, cursor, max_version, updated_at
      FROM feed_cursors WHERE feed_id=?`);
    this.upsertCursorStmt = database.prepare(`INSERT INTO feed_cursors (feed_id, cursor, max_version, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(feed_id) DO UPDATE SET cursor=excluded.cursor,
        max_version=excluded.max_version, updated_at=excluded.updated_at`);
    this.findEventsStmt = database.prepare(`SELECT event_id, op, outcome FROM feed_events
      WHERE feed_id=? AND article_key=? AND version=?`);
    this.insertEventStmt = database.prepare(`INSERT INTO feed_events
      (feed_id, article_key, version, op, payload_json, cursor, outcome, received_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    this.listEventsStmt = database.prepare(`SELECT event_id, feed_id, article_key, version, op,
      payload_json, cursor, outcome, received_at FROM feed_events
      WHERE feed_id=? ORDER BY event_id DESC LIMIT ?`);
    this.getSourceStmt = database.prepare(`SELECT feed_id, article_key, article_id, version, deleted,
      deleted_at, updated_at FROM article_sources WHERE feed_id=? AND article_key=?`);
    this.listSourcesStmt = database.prepare(`SELECT feed_id, article_key, article_id, version, deleted,
      deleted_at, updated_at FROM article_sources WHERE feed_id=?`);
    this.insertSourceStmt = database.prepare(`INSERT INTO article_sources
      (feed_id, article_key, article_id, version, deleted, deleted_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)`);
    this.updateSourceStmt = database.prepare(`UPDATE article_sources
      SET version=?, deleted=?, deleted_at=?, updated_at=? WHERE feed_id=? AND article_key=?`);
    this.insertArticleStmt = database.prepare(`INSERT INTO articles
      (feed_id, canonical_url, title, published_at, first_seen_at, last_seen_at, seen_count, visibility)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'visible')`);
    this.applyUpsertStmt = database.prepare(`UPDATE articles SET canonical_url=?, title=?,
      published_at=COALESCE(?, published_at), last_seen_at=?, missing_streak=0, visibility='visible'
      WHERE article_id=?`);
    this.markDeletedStmt = database.prepare("UPDATE articles SET visibility='quiet' WHERE article_id=?");
    this.observeStmt = database.prepare(`UPDATE articles SET last_seen_at=?, seen_count=seen_count+1,
      missing_streak=0 WHERE article_id=?`);
    this.markVisibleStmt = database.prepare("UPDATE articles SET visibility='visible' WHERE article_id=?");
    this.markMissingStmt = database.prepare(`UPDATE articles SET missing_streak=missing_streak+1,
      visibility=CASE WHEN missing_streak+1 >= ? THEN 'quiet' ELSE visibility END
      WHERE article_id=? AND visibility='visible'`);
  }

  cursor(feedId) {
    const row = this.getCursorStmt.get(feedId);
    return row ? { feedId: row.feed_id, cursor: row.cursor, maxVersion: row.max_version, updatedAt: row.updated_at } : null;
  }

  advanceCursor(feedId, cursor, maxVersion, updatedAt) {
    this.upsertCursorStmt.run(feedId, cursor, maxVersion, updatedAt);
  }

  // All stored deliveries for one (key, version); redelivery of the same op is
  // a replay, a different op at an already-seen version is a source conflict.
  deliveries(feedId, key, version) {
    return this.findEventsStmt.all(feedId, key, version)
      .map((row) => ({ eventId: row.event_id, op: row.op, outcome: row.outcome }));
  }

  recordEvent({ feedId, key, version, op, payloadJson, cursor, outcome, receivedAt }) {
    const result = this.insertEventStmt.run(feedId, key, version, op, payloadJson, cursor, outcome, receivedAt);
    return Number(result.lastInsertRowid);
  }

  events(feedId, limit = 100) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
    return this.listEventsStmt.all(feedId, safeLimit).map((row) => ({
      eventId: row.event_id,
      feedId: row.feed_id,
      key: row.article_key,
      version: row.version,
      op: row.op,
      payload: row.payload_json ? JSON.parse(row.payload_json) : null,
      cursor: row.cursor,
      outcome: row.outcome,
      receivedAt: row.received_at
    }));
  }

  source(feedId, key) {
    const row = this.getSourceStmt.get(feedId, key);
    return row ? mapSource(row) : null;
  }

  sources(feedId) {
    return this.listSourcesStmt.all(feedId).map(mapSource);
  }

  createSourcedArticle({ feedId, key, payload, version, now, observed }) {
    const result = this.insertArticleStmt.run(feedId, payload.canonicalUrl, payload.title,
      payload.publishedAt, now, now, observed ? 1 : 0);
    const articleId = Number(result.lastInsertRowid);
    this.insertSourceStmt.run(feedId, key, articleId, version, 0, null, now);
    return articleId;
  }

  applyUpsert({ feedId, key, articleId, payload, version, now, observed }) {
    this.applyUpsertStmt.run(payload.canonicalUrl, payload.title, payload.publishedAt, now, articleId);
    if (observed) this.observeStmt.run(now, articleId);
    this.updateSourceStmt.run(version, 0, null, now, feedId, key);
  }

  applyDelete({ feedId, key, articleId, version, now }) {
    this.markDeletedStmt.run(articleId);
    this.updateSourceStmt.run(version, 1, now, now, feedId, key);
  }

  // A full observation saw this key; listing presence resets the missing
  // streak and counts as one full-observation sighting.
  applyObservation({ articleId, now }) {
    this.observeStmt.run(now, articleId);
    this.markVisibleStmt.run(articleId);
  }

  applyMissing({ articleId, missingThreshold }) {
    this.markMissingStmt.run(missingThreshold, articleId);
  }
}

function mapSource(row) {
  return {
    feedId: row.feed_id,
    key: row.article_key,
    articleId: row.article_id,
    version: row.version,
    deleted: Boolean(row.deleted),
    deletedAt: row.deleted_at,
    updatedAt: row.updated_at
  };
}
