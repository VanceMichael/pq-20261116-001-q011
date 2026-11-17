import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { NewsArchive } from "../src/service/archive.js";

export const fixturePath = fileURLToPath(new URL("../fixtures/rolling-feed.html", import.meta.url));

export async function makeArchive(t, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-test-"));
  const filename = join(directory, "archive.sqlite");
  const archive = new NewsArchive(filename, options);
  t.after(() => {
    archive.close();
    return rm(directory, { recursive: true, force: true });
  });
  return { archive, directory, filename };
}

export async function fixtureHtml() {
  return readFile(fixturePath, "utf8");
}

export function page(items, { wrapper = "main", titlePrefix = "标题" } = {}) {
  const entries = items.map((item, index) => {
    const url = item.url ?? `/2026/11-16/article-${item.id}.shtml`;
    const title = item.title ?? `${titlePrefix}${item.id}`;
    const time = item.time ?? `2026-11-16T10:${String(index).padStart(2, "0")}:00+08:00`;
    return `<article><time datetime="${time}">${index}</time><a href="${url}">${title}</a></article>`;
  }).join("\n");
  return `<!doctype html><html><body><${wrapper} id="news-list">${entries}</${wrapper}></body></html>`;
}

export function standardItems(count = 4, start = 1) {
  return Array.from({ length: count }, (_, offset) => ({ id: start + offset }));
}

export const baseFeed = {
  feedId: "daily-main",
  baseUrl: "https://news.example.test/scroll-news/news1.html",
  allowedHosts: ["news.example.test"],
  minimumItems: 3,
  missingThreshold: 3
};

export const jsonFeed = {
  feedId: "wire-json",
  kind: "json",
  baseUrl: "https://wire.example.test/api/",
  allowedHosts: ["wire.example.test"],
  minimumItems: 1,
  missingThreshold: 3
};

export function jsonArticle(id, overrides = {}) {
  return {
    url: `https://wire.example.test/2026/11-17/story-${id}.shtml`,
    title: `快讯${id}`,
    publishedAt: `2026-11-17T0${Math.min(id, 9)}:00:00+08:00`,
    ...overrides
  };
}

export function captureTime(second) {
  return `2026-11-16T02:00:${String(second).padStart(2, "0")}.000Z`;
}
