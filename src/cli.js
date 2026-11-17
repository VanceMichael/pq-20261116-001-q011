import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { NewsArchive } from "./service/archive.js";
import { SyntheticJsonSource } from "./ingest/synthetic-json-source.js";
import { createHttpServer } from "./http/server.js";

export async function main(args = process.argv.slice(2), env = process.env) {
  const [command, ...parameters] = args;
  if (command === "demo") return runDemo(parameters[0] ?? "fixtures/rolling-feed.html");
  if (command === "demo-json") return runJsonDemo();
  if (command === "serve") return runServer(env);
  if (new Set(["import", "inspect", "health", "changes", "export", "audit"]).has(command)) {
    return runArchiveCommand(command, parameters, env);
  }
  throw new Error("usage: node --experimental-sqlite src/cli.js <demo [html-file] | demo-json | serve | import <feed> <html> | inspect|health|changes <feed> | export <feed> | audit>");
}

async function runDemo(filename) {
  const html = await readFile(resolve(filename), "utf8");
  const directory = await mkdtemp(join(tmpdir(), "rolling-news-demo-"));
  const databasePath = join(directory, "archive.sqlite");
  const archive = new NewsArchive(databasePath);
  try {
    archive.registerFeed({
      feedId: "synthetic-demo",
      baseUrl: "https://news.example.test/scroll-news/news1.html",
      allowedHosts: ["news.example.test"],
      minimumItems: 3
    });
    const result = archive.importSnapshot("synthetic-demo", html);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    if (!result.accepted) return 1;
    return 0;
  } finally {
    archive.close();
    await rm(directory, { recursive: true, force: true });
  }
}

async function runJsonDemo() {
  const directory = await mkdtemp(join(tmpdir(), "rolling-news-json-demo-"));
  const databasePath = join(directory, "archive.sqlite");
  const source = new SyntheticJsonSource();
  const archive = new NewsArchive(databasePath, { sourceClient: source });
  try {
    archive.registerFeed({
      feedId: "wire-json",
      kind: "json",
      baseUrl: "https://wire.example.test/api/",
      allowedHosts: ["wire.example.test"],
      minimumItems: 1
    });
    source.publish("story-1", { url: "https://wire.example.test/2026/11-17/story-1.shtml", title: "增量接口上线" });
    source.publish("story-2", { url: "https://wire.example.test/2026/11-17/story-2.shtml", title: "本地交通调整" });
    const first = await archive.syncFeed("wire-json");
    source.retract("story-1");
    source.publish("story-3", { url: "https://wire.example.test/2026/11-17/story-3.shtml", title: "夜间天气提示" });
    source.redeliverCount = 2; // the interface re-sends already-read events
    const second = await archive.syncFeed("wire-json");
    source.resyncRequested = true; // the interface demands a full re-alignment
    const third = await archive.syncFeed("wire-json");
    const status = archive.feedStatus("wire-json");
    process.stdout.write(`${JSON.stringify({
      syncs: [first, second, third].map((run) => ({
        outcome: run.outcome, batches: run.batches, resyncs: run.resyncs, counts: run.counts
      })),
      cursor: status.sync,
      statistics: status.statistics,
      items: archive.listItems("wire-json").items.map((item) => ({
        key: item.sourceKey, title: item.title, visibility: item.visibility, sourceDeleted: item.sourceDeleted
      }))
    }, null, 2)}\n`);
    return 0;
  } finally {
    archive.close();
    await rm(directory, { recursive: true, force: true });
  }
}

async function runArchiveCommand(command, parameters, env) {  const [feedId, filename, capturedAt] = parameters;
  if (command === "audit") {
    const archive = new NewsArchive(env.ARCHIVE_DB ?? "./var/archive.sqlite");
    try {
      const report = archive.auditIntegrity();
      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
      return report.status === "consistent" ? 0 : 2;
    } finally {
      archive.close();
    }
  }
  if (!feedId) throw new TypeError(`${command} requires a feed id`);
  const archive = new NewsArchive(env.ARCHIVE_DB ?? "./var/archive.sqlite");
  try {
    if (command === "import") {
      if (!filename) throw new TypeError("import requires a saved HTML file");
      const html = await readFile(resolve(filename), "utf8");
      const result = archive.importSnapshot(feedId, html, capturedAt);
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
      return result.accepted ? 0 : 2;
    }
    if (command === "inspect") return printResult(archive.feedStatus(feedId));
    if (command === "health") return printResult(archive.feedHealth(feedId));
    if (command === "changes") return printResult(archive.feedChanges(feedId));
    const result = await archive.exportCsv(feedId, process.stdout);
    if (!result.found) throw new Error(`unknown feed: ${feedId}`);
    return 0;
  } finally {
    archive.close();
  }
}

function printResult(value) {
  if (!value) throw new Error("feed was not found");
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
  return 0;
}

async function runServer(env) {
  const archive = new NewsArchive(env.ARCHIVE_DB ?? "./var/archive.sqlite");
  const port = Number(env.PORT ?? 8080);
  const host = env.HOST ?? "127.0.0.1";
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new RangeError("PORT must be a valid TCP port");
  const app = createHttpServer(archive, { host, port });
  const address = await app.listen();
  process.stdout.write(`listening on http://${address.address}:${address.port}\n`);
  const shutdown = async () => {
    await app.close();
    archive.close();
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().then((code) => { process.exitCode = code; }).catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
