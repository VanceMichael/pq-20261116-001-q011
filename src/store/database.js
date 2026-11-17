import { DatabaseSync } from "node:sqlite";
import { dirname, resolve } from "node:path";
import { mkdirSync } from "node:fs";

export const SCHEMA_VERSION = 2;

export function openDatabase(filename) {
  const path = resolve(filename);
  mkdirSync(dirname(path), { recursive: true });
  const database = new DatabaseSync(path, { timeout: 5000 });
  database.exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
  migrate(database);
  return database;
}

export function migrate(database) {
  const version = database.prepare("PRAGMA user_version").get().user_version;
  if (version > SCHEMA_VERSION) throw new Error(`database schema ${version} is newer than supported ${SCHEMA_VERSION}`);
  if (version < 1) migrateToV1(database);
  if (version < 2) migrateToV2(database);
}

function migrateToV1(database) {
  database.exec(`
    BEGIN IMMEDIATE;
    CREATE TABLE IF NOT EXISTS feeds (
      feed_id TEXT PRIMARY KEY,
      base_url TEXT NOT NULL,
      allowed_hosts_json TEXT NOT NULL,
      minimum_items INTEGER NOT NULL DEFAULT 3,
      missing_threshold INTEGER NOT NULL DEFAULT 3,
      last_accepted_at TEXT,
      last_item_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS captures (
      capture_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      fingerprint TEXT NOT NULL,
      captured_at TEXT NOT NULL,
      accepted INTEGER NOT NULL CHECK(accepted IN (0, 1)),
      candidate_count INTEGER NOT NULL,
      normalized_count INTEGER NOT NULL,
      rejected_count INTEGER NOT NULL,
      duplicate_count INTEGER NOT NULL,
      reason TEXT,
      UNIQUE(feed_id, fingerprint)
    );
    CREATE INDEX IF NOT EXISTS captures_feed_time ON captures(feed_id, captured_at DESC);
    CREATE TABLE IF NOT EXISTS articles (
      article_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      canonical_url TEXT NOT NULL,
      title TEXT NOT NULL,
      published_at TEXT,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      seen_count INTEGER NOT NULL DEFAULT 1,
      missing_streak INTEGER NOT NULL DEFAULT 0,
      visibility TEXT NOT NULL DEFAULT 'visible' CHECK(visibility IN ('visible', 'quiet')),
      review_state TEXT NOT NULL DEFAULT 'unreviewed' CHECK(review_state IN ('unreviewed', 'confirmed', 'withdrawn')),
      review_note TEXT,
      review_revision INTEGER NOT NULL DEFAULT 0,
      UNIQUE(feed_id, canonical_url)
    );
    CREATE INDEX IF NOT EXISTS articles_feed_visibility ON articles(feed_id, visibility, last_seen_at DESC);
    CREATE TABLE IF NOT EXISTS capture_items (
      capture_id INTEGER NOT NULL REFERENCES captures(capture_id) ON DELETE CASCADE,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      source_index INTEGER NOT NULL,
      PRIMARY KEY(capture_id, article_id)
    );
    CREATE TABLE IF NOT EXISTS fetch_attempts (
      attempt_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      outcome TEXT NOT NULL CHECK(outcome IN ('accepted', 'rejected', 'not_modified', 'failed')),
      http_status INTEGER,
      attempt_count INTEGER NOT NULL,
      requested_url TEXT NOT NULL,
      final_url TEXT,
      capture_id INTEGER REFERENCES captures(capture_id),
      error_code TEXT,
      error_message TEXT
    );
    CREATE INDEX IF NOT EXISTS fetch_attempts_feed_time ON fetch_attempts(feed_id, attempt_id DESC);
    CREATE TABLE IF NOT EXISTS review_events (
      event_id INTEGER PRIMARY KEY,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      prior_state TEXT NOT NULL,
      next_state TEXT NOT NULL,
      note TEXT,
      revision INTEGER NOT NULL,
      changed_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS review_events_article_time ON review_events(article_id, event_id DESC);
    PRAGMA user_version = 1;
    COMMIT;
  `);
}

function migrateToV2(database) {
  database.exec(`
    BEGIN IMMEDIATE;
    ALTER TABLE feeds ADD COLUMN kind TEXT NOT NULL DEFAULT 'html' CHECK(kind IN ('html', 'json'));
    CREATE TABLE IF NOT EXISTS feed_cursors (
      feed_id TEXT PRIMARY KEY REFERENCES feeds(feed_id),
      cursor TEXT,
      max_version INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS feed_events (
      event_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      article_key TEXT NOT NULL,
      version INTEGER NOT NULL,
      op TEXT NOT NULL CHECK(op IN ('upsert', 'delete')),
      payload_json TEXT,
      cursor TEXT NOT NULL,
      outcome TEXT NOT NULL CHECK(outcome IN ('applied', 'stale', 'invalid', 'unknown_key', 'conflict')),
      received_at TEXT NOT NULL,
      UNIQUE(feed_id, article_key, version, op)
    );
    CREATE INDEX IF NOT EXISTS feed_events_feed_time ON feed_events(feed_id, event_id DESC);
    CREATE TABLE IF NOT EXISTS article_sources (
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      article_key TEXT NOT NULL,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      version INTEGER NOT NULL,
      deleted INTEGER NOT NULL DEFAULT 0,
      deleted_at TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(feed_id, article_key),
      UNIQUE(article_id)
    );
    CREATE TABLE IF NOT EXISTS sync_runs (
      run_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      kind TEXT NOT NULL CHECK(kind IN ('events', 'alignment')),
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      outcome TEXT NOT NULL CHECK(outcome IN ('completed', 'failed')),
      stats_json TEXT NOT NULL DEFAULT '{}',
      watermark_version INTEGER,
      cursor_before TEXT,
      cursor_after TEXT,
      error TEXT
    );
    CREATE INDEX IF NOT EXISTS sync_runs_feed_time ON sync_runs(feed_id, run_id DESC);
    PRAGMA user_version = 2;
    COMMIT;
  `);
}

export function closeDatabase(database) {
  if (database?.isOpen) database.close();
}
