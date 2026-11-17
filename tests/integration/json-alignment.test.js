import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, jsonFeed, jsonArticle } from "../helpers.js";
import { SyntheticJsonSource } from "../../src/ingest/synthetic-json-source.js";

function upsert(key, version, overrides = {}) {
  return { key, version, op: "upsert", cursor: String(version), article: jsonArticle(key, overrides) };
}

function catalog(items, watermarkVersion, overrides = {}) {
  return {
    items: items.map(({ key, version, ...rest }) => ({ key, version, ...jsonArticle(key, rest) })),
    watermarkVersion,
    nextCursor: String(watermarkVersion),
    ...overrides
  };
}

test("bootstraps from a full catalog and adopts its cursor and watermark", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  const result = archive.alignCatalog("wire-json", catalog([
    { key: "s1", version: 2 }, { key: "s2", version: 3 }
  ], 3));
  assert.equal(result.counts.applied, 2);
  assert.deepEqual(result.cursor, { previous: null, current: "3", maxVersion: 3 });
  const items = archive.listItems("wire-json").items;
  assert.equal(items.length, 2);
  assert.ok(items.every((item) => item.seenCount === 1));
  // Increments resume from the catalog position; older redeliveries are stale.
  const replayed = archive.ingestEvents("wire-json", [upsert("s1", 2, { title: "旧版本" })]);
  assert.equal(replayed.counts.stale, 1);
  assert.equal(archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1").title, "快讯s1");
});

test("marks keys absent from a covered full observation as missing, then quiet", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  const snapshot = catalog([{ key: "s1", version: 1 }], 2);
  for (const round of [1, 2]) {
    const result = archive.alignCatalog("wire-json", { ...snapshot, items: [...snapshot.items] });
    assert.equal(result.counts.missing, 1);
  }
  const third = archive.alignCatalog("wire-json", catalog([{ key: "s1", version: 1 }], 2));
  assert.equal(third.counts.missing, 1);
  const missing = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s2");
  assert.equal(missing.missingStreak, 3);
  assert.equal(missing.visibility, "quiet");
  const present = archive.listItems("wire-json").items.find((item) => item.sourceKey === "s1");
  assert.equal(present.visibility, "visible");
  assert.equal(present.seenCount, 3);
});

test("does not let an older catalog overwrite increments that arrived during alignment", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  // While the alignment was being prepared, newer increments were stored.
  archive.ingestEvents("wire-json", [upsert("s1", 5, { title: "对齐期间的新版本" }), upsert("s3", 6)]);
  const result = archive.alignCatalog("wire-json", catalog([
    { key: "s1", version: 2, title: "目录里的旧标题" },
    { key: "s2", version: 2 }
  ], 4));
  // s1 keeps its newer increment data; s3 is protected from the missing sweep.
  assert.equal(result.counts.stale, 0);
  assert.equal(result.counts.observed, 2);
  assert.equal(result.counts.protected, 1);
  assert.equal(result.counts.missing, 0);
  const items = archive.listItems("wire-json").items;
  assert.equal(items.find((item) => item.sourceKey === "s1").title, "对齐期间的新版本");
  assert.equal(items.find((item) => item.sourceKey === "s1").sourceVersion, 5);
  assert.equal(items.find((item) => item.sourceKey === "s3").visibility, "visible");
  // The catalog watermark is behind the stored increments, so the cursor stays.
  assert.equal(result.cursor.current, "6");
  assert.equal(result.cursor.maxVersion, 6);
});

test("adopts the catalog cursor only when its watermark covers stored state", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1), upsert("s2", 2)]);
  const ahead = archive.alignCatalog("wire-json", catalog([{ key: "s1", version: 1 }, { key: "s2", version: 2 }], 5));
  assert.equal(ahead.cursor.current, "5");
  assert.equal(ahead.cursor.maxVersion, 5);
  const behind = archive.alignCatalog("wire-json", catalog([{ key: "s1", version: 1 }], 3));
  assert.equal(behind.cursor.current, "5");
  assert.equal(behind.cursor.maxVersion, 5);
});

test("keeps newer deletes authoritative over stale catalog listings", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1)]);
  archive.ingestEvents("wire-json", [{ key: "s1", version: 4, op: "delete", cursor: "4" }]);
  const result = archive.alignCatalog("wire-json", catalog([{ key: "s1", version: 2 }], 3));
  assert.equal(result.counts.stale, 1);
  const item = archive.listItems("wire-json").items[0];
  assert.equal(item.visibility, "quiet");
  assert.equal(item.sourceDeleted, true);
  // A catalog generated after the delete no longer lists the key and is consistent.
  const later = archive.alignCatalog("wire-json", catalog([], 5));
  assert.equal(later.counts.missing, 0);
  assert.equal(archive.listItems("wire-json").items[0].sourceDeleted, true);
});

test("rolls back the whole alignment when a write fails mid-transaction", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(jsonFeed);
  archive.ingestEvents("wire-json", [upsert("s1", 1)]);
  archive.database.exec(`CREATE TRIGGER fail_alignment BEFORE INSERT ON articles
    BEGIN SELECT RAISE(ABORT, 'simulated alignment fault'); END;`);
  assert.throws(() => archive.alignCatalog("wire-json", catalog([{ key: "s2", version: 2 }], 2)),
    /simulated alignment fault/);
  archive.database.exec("DROP TRIGGER fail_alignment");
  assert.equal(archive.listItems("wire-json").items.length, 1);
  assert.equal(archive.feedStatus("wire-json").sync.cursor, "1");
  const syncs = archive.listSyncs("wire-json");
  assert.equal(syncs[0].outcome, "failed");
  assert.equal(syncs[0].kind, "alignment");
  const retried = archive.alignCatalog("wire-json", catalog([{ key: "s2", version: 2 }], 2));
  assert.equal(retried.counts.applied, 1);
  assert.equal(archive.auditIntegrity().status, "consistent");
});

test("interleaves a demanded re-alignment with live increments without losing either", async (t) => {
  const source = new SyntheticJsonSource();
  const { archive } = await makeArchive(t, { sourceClient: source });
  archive.registerFeed(jsonFeed);
  source.publish("s1", jsonArticle("s1"));
  source.publish("s2", jsonArticle("s2"));
  await archive.syncFeed("wire-json");
  source.publish("s1", jsonArticle("s1", { title: "第二版" }));
  source.retract("s2");
  // The source demands re-alignment; its catalog snapshot is taken now but
  // returned only after a newer increment has been pushed through ingest.
  source.resyncRequested = true;
  const fetchCatalog = source.fetchCatalog.bind(source);
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  source.fetchCatalog = async (...args) => {
    const snapshot = await fetchCatalog(...args);
    await gate;
    return snapshot;
  };
  const syncing = archive.syncFeed("wire-json");
  await new Promise((resolve) => setImmediate(resolve));
  const late = source.publish("s3", jsonArticle("s3"));
  const pushed = archive.ingestEvents("wire-json", [late]);
  assert.equal(pushed.counts.applied, 1);
  release();
  const result = await syncing;
  assert.equal(result.resyncs, 1);
  const status = archive.feedStatus("wire-json");
  assert.equal(status.sync.cursor, "5");
  assert.equal(status.sync.maxVersion, 5);
  const items = archive.listItems("wire-json").items;
  assert.equal(items.find((item) => item.sourceKey === "s1").title, "第二版");
  // s2's delete event was never delivered (the cursor moved past it), so the
  // catalog's silence is the only evidence: confirmed-missing, not deleted.
  const s2 = items.find((item) => item.sourceKey === "s2");
  assert.equal(s2.sourceDeleted, false);
  assert.equal(s2.missingStreak, 1);
  assert.equal(s2.visibility, "visible");
  assert.equal(items.find((item) => item.sourceKey === "s3").visibility, "visible");
  const alignments = archive.listSyncs("wire-json").filter((run) => run.kind === "alignment");
  assert.equal(alignments.length, 1);
  assert.equal(alignments[0].stats.protected, 1);
  assert.equal(alignments[0].stats.missing, 1);
  assert.equal(archive.auditIntegrity().status, "consistent");
});
