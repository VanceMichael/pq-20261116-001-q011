import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { NewsArchive } from "../../src/service/archive.js";
import { jsonFeed, jsonArticle } from "../helpers.js";

// The frozen v1 schema, copied from the original baseline so the upgrade path
// is exercised against real historical databases.
const V1_SCHEMA = `
  CREATE TABLE feeds (
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
  CREATE TABLE captures (
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
  CREATE TABLE articles (
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
  CREATE TABLE capture_items (
    capture_id INTEGER NOT NULL REFERENCES captures(capture_id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL REFERENCES articles(article_id),
    source_index INTEGER NOT NULL,
    PRIMARY KEY(capture_id, article_id)
  );
  CREATE TABLE fetch_attempts (
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
  CREATE TABLE review_events (
    event_id INTEGER PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES articles(article_id),
    prior_state TEXT NOT NULL,
    next_state TEXT NOT NULL,
    note TEXT,
    revision INTEGER NOT NULL,
    changed_at TEXT NOT NULL
  );
  PRAGMA user_version = 1;
`;

test("upgrades a v1 database in place and keeps HTML archive data intact", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-migration-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, "archive.sqlite");
  const legacy = new DatabaseSync(filename);
  legacy.exec(V1_SCHEMA);
  legacy.prepare(`INSERT INTO feeds (feed_id, base_url, allowed_hosts_json, minimum_items, missing_threshold,
    last_accepted_at, last_item_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run("daily-main", "https://news.example.test/scroll-news/news1.html", '["news.example.test"]',
      3, 3, "2026-11-16T02:00:01.000Z", 1, "2026-11-16T02:00:01.000Z", "2026-11-16T02:00:01.000Z");
  legacy.prepare(`INSERT INTO articles (feed_id, canonical_url, title, first_seen_at, last_seen_at)
    VALUES (?, ?, ?, ?, ?)`)
    .run("daily-main", "https://news.example.test/2026/11-16/article-1.shtml", "旧闻",
      "2026-11-16T02:00:01.000Z", "2026-11-16T02:00:01.000Z");
  legacy.prepare(`INSERT INTO captures (feed_id, fingerprint, captured_at, accepted, candidate_count,
    normalized_count, rejected_count, duplicate_count) VALUES (?, ?, ?, 1, 1, 1, 0, 0)`)
    .run("daily-main", "legacy-fingerprint", "2026-11-16T02:00:01.000Z");
  legacy.prepare("INSERT INTO capture_items (capture_id, article_id, source_index) VALUES (1, 1, 0)").run();
  legacy.close();

  const archive = new NewsArchive(filename);
  t.after(() => archive.close());
  const feed = archive.feedStatus("daily-main");
  assert.equal(feed.kind, "html");
  assert.equal(feed.lastAcceptedAt, "2026-11-16T02:00:01.000Z");
  assert.equal(archive.listItems("daily-main").items[0].title, "旧闻");
  assert.equal(archive.listItems("daily-main").items[0].sourceKey, null);
  // The upgraded database accepts the new JSON source type right away.
  archive.registerFeed(jsonFeed);
  const result = archive.ingestEvents("wire-json", [
    { key: "s1", version: 1, op: "upsert", cursor: "1", article: jsonArticle("s1") }
  ]);
  assert.equal(result.counts.applied, 1);
  assert.equal(archive.feedStatus("wire-json").sync.cursor, "1");
  assert.equal(archive.auditIntegrity().status, "consistent");
});

test("detects a JSON cursor that regressed behind the event evidence", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-audit-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const archive = new NewsArchive(join(directory, "archive.sqlite"));
  t.after(() => archive.close());
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [
    { key: "s1", version: 1, op: "upsert", cursor: "1", article: jsonArticle("s1") },
    { key: "s2", version: 2, op: "upsert", cursor: "2", article: jsonArticle("s2") }
  ]);
  assert.equal(archive.auditIntegrity().status, "consistent");
  archive.database.prepare("UPDATE feed_cursors SET max_version=0 WHERE feed_id='wire-json'").run();
  const report = archive.auditIntegrity();
  assert.equal(report.status, "inconsistent");
  const check = report.checks.find((entry) => entry.name === "json_cursor_versions");
  assert.equal(check.passed, false);
  assert.equal(check.examples[0].logged_max, 2);
});
