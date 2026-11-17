import test from "node:test";
import assert from "node:assert/strict";
import { parseEventBatch, parseCatalog, normalizePayload } from "../../src/ingest/json-feed.js";

const feed = {
  baseUrl: "https://wire.example.test/api/",
  allowedHosts: ["wire.example.test"]
};

test("parses a well-formed upsert and delete event batch", () => {
  const [upsert, deleted] = parseEventBatch([
    { key: "story-1", version: 3, op: "upsert", cursor: "3", article: { url: "/a.shtml", title: "标题" } },
    { key: "story-2", version: 4, op: "delete", cursor: "4" }
  ]);
  assert.equal(upsert.key, "story-1");
  assert.equal(upsert.article.title, "标题");
  assert.equal(deleted.article, null);
});

test("rejects structurally malformed events before anything is stored", () => {
  assert.throws(() => parseEventBatch("not-an-array"), TypeError);
  assert.throws(() => parseEventBatch([{ key: "a", version: 1, op: "upsert", cursor: "1", article: "junk" }]), /article/);
  assert.throws(() => parseEventBatch([{ key: "a", version: 1, op: "upsert", cursor: "1", article: { title: "无地址" } }]), /url/);
  assert.throws(() => parseEventBatch([{ key: "", version: 1, op: "upsert", cursor: "1" }]), RangeError);
  assert.throws(() => parseEventBatch([{ key: "a", version: 0, op: "upsert", cursor: "1" }]), TypeError);
  assert.throws(() => parseEventBatch([{ key: "a", version: 1.5, op: "upsert", cursor: "1" }]), TypeError);
  assert.throws(() => parseEventBatch([{ key: "a", version: 1, op: "merge", cursor: "1" }]), TypeError);
  assert.throws(() => parseEventBatch([{ key: "a", version: 1, op: "upsert" }]), /cursor/);
});

test("parses a full alignment catalog with watermark and next cursor", () => {
  const catalog = parseCatalog({
    watermarkVersion: 9,
    nextCursor: "9",
    observedAt: "2026-11-17T01:00:00Z",
    items: [{ key: "story-1", version: 4, url: "/a.shtml", title: "标题" }]
  });
  assert.equal(catalog.watermarkVersion, 9);
  assert.equal(catalog.nextCursor, "9");
  assert.equal(catalog.observedAt, "2026-11-17T01:00:00.000Z");
  assert.equal(catalog.items[0].article.url, "/a.shtml");
});

test("rejects catalogs without a trustworthy watermark or cursor", () => {
  assert.throws(() => parseCatalog(null), TypeError);
  assert.throws(() => parseCatalog({ items: [] }), /watermarkVersion/);
  assert.throws(() => parseCatalog({ items: [], watermarkVersion: -1 }), TypeError);
  assert.throws(() => parseCatalog({ items: [], watermarkVersion: 1 }), /cursor/);
  assert.throws(() => parseCatalog({ items: [], watermarkVersion: 1, nextCursor: "1", observedAt: "junk" }), TypeError);
});

test("normalizes payloads against the feed allowlist like HTML candidates", () => {
  const payload = normalizePayload(
    { url: "/2026/11-17/a.shtml?utm_source=x#frag", title: "  多  空 格  ", publishedAt: "bad-date" },
    feed
  );
  assert.equal(payload.canonicalUrl, "https://wire.example.test/2026/11-17/a.shtml");
  assert.equal(payload.title, "多 空 格");
  assert.equal(payload.publishedAt, null);
  assert.throws(() => normalizePayload({ url: "https://evil.example.net/a.shtml", title: "标题" }, feed), TypeError);
  assert.throws(() => normalizePayload({ url: "/a.shtml", title: "x" }, feed), RangeError);
});
