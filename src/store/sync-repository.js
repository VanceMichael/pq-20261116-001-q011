export class SyncRepository {
  constructor(database) {
    this.database = database;
    this.insert = database.prepare(`INSERT INTO sync_runs
      (feed_id, kind, started_at, finished_at, outcome, stats_json, watermark_version,
       cursor_before, cursor_after, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    this.listQuery = database.prepare(`SELECT run_id, feed_id, kind, started_at, finished_at, outcome,
      stats_json, watermark_version, cursor_before, cursor_after, error
      FROM sync_runs WHERE feed_id=? ORDER BY run_id DESC LIMIT ?`);
    this.summaryQuery = database.prepare(`SELECT COUNT(*) AS total,
      SUM(CASE WHEN outcome='completed' THEN 1 ELSE 0 END) AS accepted,
      SUM(CASE WHEN outcome='failed' THEN 1 ELSE 0 END) AS failed,
      MAX(CASE WHEN outcome='completed' THEN finished_at END) AS last_success_at,
      MAX(finished_at) AS last_attempt_at
      FROM sync_runs WHERE feed_id=? AND finished_at>=?`);
  }

  record({ feedId, kind, startedAt, finishedAt, outcome, stats = {}, watermarkVersion = null,
    cursorBefore = null, cursorAfter = null, error = null }) {
    const result = this.insert.run(feedId, kind, startedAt, finishedAt, outcome,
      JSON.stringify(stats), watermarkVersion, cursorBefore, cursorAfter, error);
    return Number(result.lastInsertRowid);
  }

  recent(feedId, limit = 20) {
    const safeLimit = Math.max(1, Math.min(100, Number(limit) || 20));
    return this.listQuery.all(feedId, safeLimit).map(mapRun);
  }

  // Same shape as FetchRepository.summarySince so health reporting treats
  // HTML fetches and JSON sync runs uniformly.
  summarySince(feedId, since) {
    const result = this.summaryQuery.get(feedId, since);
    return {
      total: result.total,
      accepted: result.accepted ?? 0,
      rejected: 0,
      failed: result.failed ?? 0,
      notModified: 0,
      lastSuccessAt: result.last_success_at,
      lastAttemptAt: result.last_attempt_at
    };
  }
}

function mapRun(row) {
  return {
    runId: row.run_id,
    feedId: row.feed_id,
    kind: row.kind,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    outcome: row.outcome,
    stats: JSON.parse(row.stats_json),
    watermarkVersion: row.watermark_version,
    cursorBefore: row.cursor_before,
    cursorAfter: row.cursor_after,
    error: row.error
  };
}
