import { canonicalizeUrl, cleanTitle, normalizePublishedAt } from "../feed/normalize.js";

export const MAX_BATCH_EVENTS = 5000;
export const MAX_CATALOG_ITEMS = 50_000;

// Structural validation only: a malformed batch is rejected as a client error
// before anything is stored. Payload *data* problems (bad URL host, short
// title) are handled per event by the service so one poisoned event cannot
// block the stream; the event is still logged as source evidence.
export function parseEventBatch(input) {
  const list = Array.isArray(input) ? input : input?.events;
  if (!Array.isArray(list)) throw new TypeError("event batch must be an array or an object with an events array");
  if (list.length > MAX_BATCH_EVENTS) throw new RangeError(`event batch cannot exceed ${MAX_BATCH_EVENTS} events`);
  return list.map((raw, index) => parseEvent(raw, index));
}

export function parseEvent(raw, index = 0) {
  const where = `event[${index}]`;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new TypeError(`${where} must be an object`);
  return {
    key: parseKey(raw.key, where),
    version: parseVersion(raw.version, where),
    op: parseOp(raw.op, where),
    cursor: parseCursor(raw.cursor, where),
    article: raw.article === undefined || raw.article === null ? null : parseArticleEnvelope(raw.article, where)
  };
}

export function parseCatalog(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new TypeError("catalog must be an object");
  if (!Array.isArray(input.items)) throw new TypeError("catalog must contain an items array");
  if (input.items.length > MAX_CATALOG_ITEMS) {
    throw new RangeError(`catalog cannot exceed ${MAX_CATALOG_ITEMS} items`);
  }
  const watermarkVersion = input.watermarkVersion;
  if (!Number.isSafeInteger(watermarkVersion) || watermarkVersion < 0) {
    throw new TypeError("catalog watermarkVersion must be a non-negative integer");
  }
  return {
    items: input.items.map((raw, index) => parseCatalogItem(raw, index)),
    watermarkVersion,
    nextCursor: parseCursor(input.nextCursor, "catalog"),
    observedAt: parseObservedAt(input.observedAt)
  };
}

function parseCatalogItem(raw, index) {
  const where = `catalog item[${index}]`;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new TypeError(`${where} must be an object`);
  return {
    key: parseKey(raw.key, where),
    version: parseVersion(raw.version, where),
    article: parseArticleEnvelope(raw, where)
  };
}

// Feed-aware payload normalization. Throws TypeError/RangeError when the
// source payload itself is unusable; callers decide whether to record the
// event as invalid rather than fail the whole batch.
export function normalizePayload(article, feed) {
  return {
    canonicalUrl: canonicalizeUrl(article.url, feed.baseUrl, feed.allowedHosts),
    title: cleanTitle(article.title),
    publishedAt: normalizePublishedAt(article.publishedAt)
  };
}

function parseArticleEnvelope(raw, where) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new TypeError(`${where} article must be an object`);
  if (typeof raw.url !== "string" || !raw.url) throw new TypeError(`${where} article url must be a non-empty string`);
  if (typeof raw.title !== "string") throw new TypeError(`${where} article title must be a string`);
  return { url: raw.url, title: raw.title, publishedAt: raw.publishedAt ?? null };
}

function parseKey(value, where) {
  if (typeof value !== "string") throw new TypeError(`${where} key must be a string`);
  const key = value.normalize("NFC").trim();
  if (!key || key.length > 200 || /[\u0000-\u001f\u007f]/.test(key)) {
    throw new RangeError(`${where} key must contain 1 to 200 visible characters`);
  }
  return key;
}

function parseVersion(value, where) {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${where} version must be a positive integer`);
  return value;
}

function parseOp(value, where) {
  if (!new Set(["upsert", "delete"]).has(value)) throw new TypeError(`${where} op must be upsert or delete`);
  return value;
}

function parseCursor(value, where) {
  if (typeof value !== "string" || !value || value.length > 500) {
    throw new TypeError(`${where} cursor must be a string of 1 to 500 characters`);
  }
  return value;
}

function parseObservedAt(value) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("catalog observedAt must be a valid date-time");
  return parsed.toISOString();
}
