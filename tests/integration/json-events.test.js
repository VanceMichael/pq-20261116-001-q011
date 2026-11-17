import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, page, standardItems, baseFeed, jsonFeed, jsonArticle, captureTime } from "../helpers.js";
import { createHttpServer } from "../../src/http/server.js";

function upsert(key, version, overrides = {}) {
  return { key, version, op: "upsert", cursor: String(version), article: jsonArticle(key, overrides) };
}

test("applies upsert events and exposes the stable source key on archived items", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  const result = archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  assert.equal(result.outcome, "completed");
  assert.deepEqual(result.counts, { received: 2, applied: 2, replay: 0, stale: 0, conflict: 0, invalid: 0, unknownKey: 0 });
  assert.deepEqual(result.cursor, { previous: null, current: "2", maxVersion: 2 });
  const items = archive.listItems("wire-json").items;
  assert.equal(items.length, 2);
  const first = items.find((item) => item.sourceKey === "s1");
  assert.equal(first.canonicalUrl, "https://wire.example.test/2026/11-17/story-s1.shtml");
  assert.equal(first.sourceVersion, 1);
  assert.equal(first.sourceDeleted, false);
  assert.equal(first.visibility, "visible");
});

test("treats redelivered events as replays without changing stored state or cursor", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  const batch = [upsert("s1", 1), upsert("s2", 2, { title: "原始标题" })];
  archive.ingestEvents("wire-json", batch);
  const replayed = archive.ingestEvents("wire-json", batch);
  assert.equal(replayed.counts.applied, 0);
  assert.equal(replayed.counts.replay, 2);
  assert.equal(replayed.cursor.current, "2");
  assert.equal(replayed.cursor.previous, "2");
  const item = archive.listItems("wire-json").items.find((entry) => entry.sourceKey === "s2");
  assert.equal(item.title, "原始标题");
  assert.equal(item.sourceVersion, 2);
  assert.equal(archive.listEvents("wire-json").length, 2);
});

test("never regresses a key when same-key versions arrive out of order", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 5, { title: "第五版" })]);
  const result = archive.ingestEvents("wire-json", [upsert("s1", 3, { title: "第三版" }), upsert("s1", 4, { title: "第四版" })]);
  assert.deepEqual(result.events.map((event) => event.outcome), ["stale", "stale"]);
  const item = archive.listItems("wire-json").items[0];
  assert.equal(item.title, "第五版");
  assert.equal(item.sourceVersion, 5);
  assert.equal(result.cursor.current, "5");
  assert.equal(archive.listEvents("wire-json").length, 3);
});

test("advances the cursor only to versions actually persisted in the same round", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  // A reconnect replay mixes already-read events with genuinely new ones.
  const mixed = archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2), upsert("s3", 3)]);
  assert.deepEqual(mixed.events.map((event) => event.outcome), ["replay", "replay", "applied"]);
  assert.equal(mixed.cursor.current, "3");
  // A pure redelivery of old events must not move the cursor backwards.
  const stale = archive.ingestEvents("wire-json", [upsert("s1", 1)]);
  assert.equal(stale.cursor.current, "3");
  assert.equal(stale.cursor.maxVersion, 3);
});

test("keeps delete events as source evidence without erasing the article or its reviews", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  const article = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1");
  archive.reviewArticle("wire-json", article.articleId, { state: "confirmed", note: "已核实", expectedRevision: 0 });
  const deleted = archive.ingestEvents("wire-json", [{ key: "s1", version: 3, op: "delete", cursor: "3" }]);
  assert.equal(deleted.counts.applied, 1);
  const after = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1");
  assert.equal(after.visibility, "quiet");
  assert.equal(after.sourceDeleted, true);
  assert.equal(after.reviewState, "confirmed");
  const history = archive.reviewEvents("wire-json", article.articleId);
  assert.equal(history.events.length, 1);
  assert.equal(history.events[0].note, "已核实");
  const events = archive.listEvents("wire-json");
  assert.equal(events[0].op, "delete");
  assert.equal(events[0].outcome, "applied");
  // A newer upsert revives the same key; the older delete stays in the log.
  archive.ingestEvents("wire-json", [upsert("s1", 4, { title: "恢复上线" })]);
  const revived = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1");
  assert.equal(revived.visibility, "visible");
  assert.equal(revived.sourceDeleted, false);
  assert.equal(revived.title, "恢复上线");
  assert.equal(revived.reviewState, "confirmed");
});

test("records deletes for unknown keys and conflicting ops as evidence only", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  const unknown = archive.ingestEvents("wire-json", [{ key: "ghost", version: 1, op: "delete", cursor: "1" }]);
  assert.equal(unknown.counts.unknownKey, 1);
  assert.equal(archive.listItems("wire-json").items.length, 0);
  archive.ingestEvents("wire-json", [upsert("s1", 2)]);
  const conflict = archive.ingestEvents("wire-json", [{ key: "s1", version: 2, op: "delete", cursor: "2" }]);
  assert.equal(conflict.counts.conflict, 1);
  const item = archive.listItems("wire-json").items[0];
  assert.equal(item.visibility, "visible");
  assert.equal(item.sourceDeleted, false);
});

test("logs invalid payloads as evidence without blocking the rest of the stream", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  const result = archive.ingestEvents("wire-json", [
    { key: "bad", version: 1, op: "upsert", cursor: "1", article: { url: "https://evil.example.net/x.shtml", title: "越域" } },
    upsert("good", 2)
  ]);
  assert.deepEqual(result.events.map((event) => event.outcome), ["invalid", "applied"]);
  assert.equal(result.cursor.current, "2");
  const items = archive.listItems("wire-json").items;
  assert.equal(items.length, 1);
  assert.equal(items[0].sourceKey, "good");
  const logged = archive.listEvents("wire-json");
  assert.equal(logged.find((event) => event.key === "bad").outcome, "invalid");
});

test("never marks articles missing just because an increment did not mention them", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  for (let version = 3; version <= 6; version += 1) {
    archive.ingestEvents("wire-json", [upsert("s1", version, { title: `第${version}版` })]);
  }
  const both = archive.listItems("wire-json").items;
  assert.equal(both.length, 2);
  const unmentioned = both.find((item) => item.sourceKey === "s2");
  assert.equal(unmentioned.visibility, "visible");
  assert.equal(unmentioned.missingStreak, 0);
});

test("keeps HTML and JSON intake strictly separated per feed", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.registerFeed(jsonFeed);
  assert.throws(() => archive.ingestEvents("daily-main", [upsert("s1", 1)]), { statusCode: 409 });
  assert.throws(() => archive.importSnapshot("wire-json", page(standardItems(4)), captureTime(1)), { statusCode: 409 });
  await assert.rejects(archive.refreshFeed("wire-json"), { statusCode: 409 });
  assert.throws(() => archive.registerFeed({ ...jsonFeed, feedId: "daily-main" }), { statusCode: 409 });
  // The HTML feed still works exactly as before alongside the JSON feed.
  const imported = archive.importSnapshot("daily-main", page(standardItems(4)), captureTime(1));
  assert.equal(imported.accepted, true);
  assert.equal(archive.listItems("daily-main").items.length, 4);
  assert.equal(archive.listItems("wire-json").items.length, 0);
});

test("exports JSON feed articles to CSV and reviews them like HTML ones", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  const article = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1");
  const reviewed = archive.reviewArticle("wire-json", article.articleId, { state: "confirmed", expectedRevision: 0 });
  assert.equal(reviewed.status, 200);
  const chunks = [];
  const writable = {
    write(chunk) { chunks.push(chunk); return true; },
    destroyed: false,
    writableEnded: false
  };
  const result = await archive.exportCsv("wire-json", writable);
  assert.equal(result.rows, 2);
  const csv = chunks.join("");
  assert.match(csv, /快讯s1/);
  assert.match(csv, /confirmed/);
});

test("serves JSON intake over HTTP with cursor, event log and sync run evidence", async (t) => {
  const { archive } = await makeArchive(t);
  const app = createHttpServer(archive, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  const base = `http://127.0.0.1:${address.port}`;
  const created = await fetch(`${base}/v1/feeds`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(jsonFeed)
  });
  assert.equal(created.status, 201);
  assert.equal((await created.json()).feed.kind, "json");
  const posted = await fetch(`${base}/v1/feeds/wire-json/events`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ events: [upsert("s1", 1), upsert("s2", 2)] })
  });
  assert.equal(posted.status, 200);
  assert.equal((await posted.json()).counts.applied, 2);
  const events = await (await fetch(`${base}/v1/feeds/wire-json/events`)).json();
  assert.equal(events.events.length, 2);
  const syncs = await (await fetch(`${base}/v1/feeds/wire-json/syncs`)).json();
  assert.equal(syncs.syncs.length, 1);
  assert.equal(syncs.syncs[0].kind, "events");
  const status = await (await fetch(`${base}/v1/feeds/wire-json`)).json();
  assert.equal(status.health, "healthy");
  assert.equal(status.sync.cursor, "2");
  assert.equal(status.sync.maxVersion, 2);
  const missing = await fetch(`${base}/v1/feeds/unknown-json/events`, { method: "POST", body: "[]" });
  assert.equal(missing.status, 404);
});
