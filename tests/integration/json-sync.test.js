import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeArchive, jsonFeed, jsonArticle } from "../helpers.js";
import { NewsArchive } from "../../src/service/archive.js";
import { SyntheticJsonSource } from "../../src/ingest/synthetic-json-source.js";
import { createHttpServer } from "../../src/http/server.js";

test("syncs a JSON feed end to end from the synthetic source", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  source.publish("s2", jsonArticle("s2"));
  source.retract("s1");
  const result = await archive.syncFeed("wire-json");
  assert.equal(result.outcome, "completed");
  assert.equal(result.batches, 1);
  assert.equal(result.counts.applied, 3);
  assert.deepEqual(result.cursor, { feedId: "wire-json", cursor: "3", maxVersion: 3, updatedAt: result.cursor.updatedAt });
  const items = archive.listItems("wire-json").items;
  assert.equal(items.find((item) => item.sourceKey === "s1").sourceDeleted, true);
  assert.equal(items.find((item) => item.sourceKey === "s2").visibility, "visible");
  // A follow-up sync with no new events is a no-op.
  const idle = await archive.syncFeed("wire-json");
  assert.equal(idle.batches, 0);
  assert.equal(idle.counts.applied, 0);
});

test("survives a restart with cursor and versions intact, then resumes incrementally", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-restart-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, "archive.sqlite");
  const source = new SyntheticJsonSource();
  const first = new NewsArchive(filename, { sourceClient: source });
  first.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  source.publish("s2", jsonArticle("s2"));
  await first.syncFeed("wire-json");
  assert.equal(first.feedStatus("wire-json").sync.cursor, "2");
  first.close();

  const reopened = new NewsArchive(filename, { sourceClient: source });
  t.after(() => reopened.close());
  assert.equal(reopened.feedStatus("wire-json").sync.cursor, "2");
  assert.equal(reopened.listItems("wire-json").items.length, 2);
  source.publish("s1", jsonArticle("s1", { title: "重启后的新版本" }));
  source.redeliverCount = 2; // the interface re-sends the two already-read events
  const result = await reopened.syncFeed("wire-json");
  assert.equal(result.counts.replay, 2);
  assert.equal(result.counts.applied, 1);
  assert.equal(result.cursor.cursor, "3");
  const item = reopened.listItems("wire-json").items.find((entry) => entry.sourceKey === "s1");
  assert.equal(item.title, "重启后的新版本");
  assert.equal(item.sourceVersion, 3);
  assert.equal(reopened.listEvents("wire-json").length, 3);
  assert.equal(reopened.auditIntegrity().status, "consistent");
});

test("keeps the cursor untouched across disconnects and replays from it after recovery", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  await archive.syncFeed("wire-json");
  source.publish("s2", jsonArticle("s2"));
  source.failuresRemaining = 1; // the connection drops before the new event arrives
  await assert.rejects(archive.syncFeed("wire-json"), { code: "source_unavailable" });
  assert.equal(archive.feedStatus("wire-json").sync.cursor, "1");
  const failed = archive.listSyncs("wire-json").find((run) => run.outcome === "failed");
  assert.ok(failed);
  const recovered = await archive.syncFeed("wire-json");
  assert.equal(recovered.counts.applied, 1);
  assert.equal(recovered.cursor.cursor, "2");
  assert.equal(archive.listItems("wire-json").items.length, 2);
});

test("replays from an old cursor after state loss without duplicating articles", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  source.publish("s2", jsonArticle("s2"));
  source.publish("s1", jsonArticle("s1", { title: "第二版" }));
  await archive.syncFeed("wire-json");
  // Simulate an operator restoring an older cursor backup: the source is
  // asked to replay everything from the beginning again.
  const replay = await source.fetchEvents({ cursor: "0" });
  assert.equal(replay.events.length, 3);
  const result = archive.ingestEvents("wire-json", replay.events);
  assert.equal(result.counts.replay, 3);
  assert.equal(result.counts.stale, 0);
  assert.equal(result.counts.applied, 0);
  const items = archive.listItems("wire-json").items;
  assert.equal(items.length, 2);
  assert.equal(items.find((item) => item.sourceKey === "s1").title, "第二版");
  assert.equal(archive.feedStatus("wire-json").sync.cursor, "3");
});

test("performs a demanded re-alignment and resumes increments from the new position", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  source.publish("s2", jsonArticle("s2"));
  await archive.syncFeed("wire-json");
  source.retract("s2");
  source.publish("s3", jsonArticle("s3"));
  source.resyncRequested = true;
  const result = await archive.syncFeed("wire-json");
  assert.equal(result.resyncs, 1);
  assert.equal(result.cursor.cursor, "4");
  const items = archive.listItems("wire-json").items;
  assert.equal(items.find((item) => item.sourceKey === "s2").missingStreak, 1);
  assert.equal(items.find((item) => item.sourceKey === "s3").visibility, "visible");
  source.publish("s1", jsonArticle("s1", { title: "重对齐后的更新" }));
  const after = await archive.syncFeed("wire-json");
  assert.equal(after.counts.applied, 1);
  assert.equal(after.cursor.cursor, "5");
  assert.equal(archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1").title, "重对齐后的更新");
});

test("forces re-alignment when the source compacted away the stored cursor", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  await archive.syncFeed("wire-json");
  source.publish("s2", jsonArticle("s2"));
  source.publish("s3", jsonArticle("s3"));
  source.oldestRetainedVersion = 2; // our cursor "1" fell out of the retained window
  const result = await archive.syncFeed("wire-json");
  assert.equal(result.resyncs, 1);
  assert.equal(result.cursor.cursor, "3");
  assert.equal(archive.listItems("wire-json").items.length, 3);
});

test("coalesces concurrent sync requests into one in-flight pull", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  let calls = 0;
  const fetchEvents = source.fetchEvents.bind(source);
  source.fetchEvents = async (...args) => {
    calls += 1;
    await new Promise((resolve) => setImmediate(resolve));
    return fetchEvents(...args);
  };
  const [left, right] = await Promise.all([archive.syncFeed("wire-json"), archive.syncFeed("wire-json")]);
  assert.equal(left, right);
  assert.equal(calls >= 1, true);
  assert.equal(archive.feedStatus("wire-json").sync.cursor, "1");
});

test("reports JSON feed health from sync runs rather than HTML fetch attempts", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  assert.equal(archive.feedHealth("wire-json").status, "awaiting_capture");
  source.publish("s1", jsonArticle("s1"));
  await archive.syncFeed("wire-json");
  const health = archive.feedHealth("wire-json");
  assert.equal(health.status, "healthy");
  assert.equal(health.totals.attempts, 1);
  assert.equal(health.totals.accepted, 1);
});

test("drives sync and alignment through the HTTP endpoints", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  const app = createHttpServer(archive, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  const base = `http://127.0.0.1:${address.port}`;
  await fetch(`${base}/v1/feeds`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(jsonFeed)
  });
  source.publish("s1", jsonArticle("s1"));
  const synced = await fetch(`${base}/v1/feeds/wire-json/sync`, { method: "POST" });
  assert.equal(synced.status, 200);
  assert.equal((await synced.json()).counts.applied, 1);
  source.resyncRequested = true;
  const realigned = await fetch(`${base}/v1/feeds/wire-json/sync`, { method: "POST" });
  assert.equal((await realigned.json()).resyncs, 1);
  const pushed = await fetch(`${base}/v1/feeds/wire-json/alignment`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      items: [{ key: "s1", version: 1, url: "https://wire.example.test/2026/11-17/story-s1.shtml", title: "快讯s1" }],
      watermarkVersion: 1,
      nextCursor: "1"
    })
  });
  assert.equal(pushed.status, 200);
  assert.equal((await pushed.json()).counts.observed, 1);
  const syncs = await (await fetch(`${base}/v1/feeds/wire-json/syncs`)).json();
  assert.ok(syncs.syncs.some((run) => run.kind === "alignment"));
  const rejected = await fetch(`${base}/v1/feeds/wire-json/refresh`, { method: "POST" });
  assert.equal(rejected.status, 409);
});
