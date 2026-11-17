import { openDatabase, closeDatabase } from "../store/database.js";
import { FeedRepository } from "../store/feed-repository.js";
import { CaptureRepository } from "../store/capture-repository.js";
import { ArticleRepository } from "../store/article-repository.js";
import { FetchRepository } from "../store/fetch-repository.js";
import { EventRepository } from "../store/event-repository.js";
import { SyncRepository } from "../store/sync-repository.js";
import { ImportService } from "../ingest/import-service.js";
import { JsonFeedService } from "../ingest/json-feed-service.js";
import { PageFetcher } from "../acquisition/page-fetcher.js";
import { RefreshService } from "./refresh-service.js";
import { SyncService } from "./sync-service.js";
import { ChangeReportService } from "./change-report.js";
import { HealthReportService } from "./health-report.js";
import { ReviewRepository } from "../store/review-repository.js";
import { ReviewService } from "./review-service.js";
import { CsvExporter } from "../export/csv-exporter.js";
import { ArticleTimelineService } from "./article-timeline.js";
import { IntegrityAuditService } from "./integrity-audit.js";

export class NewsArchive {
  constructor(filename, options = {}) {
    this.clock = options.clock ?? (() => new Date());
    this.database = openDatabase(filename);
    this.feeds = new FeedRepository(this.database);
    this.captures = new CaptureRepository(this.database);
    this.articles = new ArticleRepository(this.database);
    this.fetches = new FetchRepository(this.database);
    this.events = new EventRepository(this.database);
    this.syncs = new SyncRepository(this.database);
    this.reviews = new ReviewRepository(this.database);
    this.imports = new ImportService({
      database: this.database,
      feeds: this.feeds,
      captures: this.captures,
      articles: this.articles,
      clock: this.clock
    });
    this.jsonFeeds = new JsonFeedService({
      database: this.database,
      feeds: this.feeds,
      events: this.events,
      syncs: this.syncs,
      clock: this.clock
    });
    this.refreshes = new RefreshService({
      feeds: this.feeds,
      fetches: this.fetches,
      fetcher: options.fetcher ?? new PageFetcher(),
      imports: this.imports,
      clock: this.clock
    });
    this.syncService = new SyncService({
      feeds: this.feeds,
      jsonFeeds: this.jsonFeeds,
      syncs: this.syncs,
      sourceClient: options.sourceClient ?? null,
      clock: this.clock
    });
    this.changes = new ChangeReportService({ feeds: this.feeds, captures: this.captures, articles: this.articles });
    this.health = new HealthReportService({ feeds: this.feeds, fetches: this.fetches, syncs: this.syncs, clock: this.clock });
    this.reviewService = new ReviewService({ feeds: this.feeds, reviews: this.reviews, clock: this.clock });
    this.csvExporter = new CsvExporter({ database: this.database, feeds: this.feeds, articles: this.articles });
    this.timelines = new ArticleTimelineService({ feeds: this.feeds, reviews: this.reviews, articles: this.articles });
    this.integrityAudit = new IntegrityAuditService(this.database);
  }

  registerFeed(input) {
    return this.feeds.register(input, this.now());
  }

  importSnapshot(feedId, html, capturedAt) {
    return this.imports.importHtml(feedId, html, capturedAt);
  }

  refreshFeed(feedId) {
    return this.refreshes.refresh(feedId);
  }

  ingestEvents(feedId, input) {
    return this.jsonFeeds.ingestEvents(feedId, input);
  }

  alignCatalog(feedId, catalog) {
    return this.jsonFeeds.alignCatalog(feedId, catalog);
  }

  syncFeed(feedId) {
    return this.syncService.sync(feedId);
  }

  listEvents(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.events.events(feedId, limit);
  }

  listSyncs(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.syncs.recent(feedId, limit);
  }

  feedStatus(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) return null;
    if (feed.kind === "json") {
      const latestSync = this.syncs.recent(feedId, 1)[0] ?? null;
      return {
        ...feed,
        health: latestSync ? latestSync.outcome === "completed" ? "healthy" : "degraded" : "awaiting_sync",
        latestSync,
        sync: this.events.cursor(feedId),
        statistics: this.articles.stats(feedId),
        collectionHealth: this.health.report(feedId)
      };
    }
    const latest = this.captures.recent(feedId, 1)[0] ?? null;
    return {
      ...feed,
      health: latest ? latest.accepted ? "healthy" : "degraded" : "awaiting_capture",
      latestAttempt: latest,
      statistics: this.articles.stats(feedId),
      collectionHealth: this.health.report(feedId)
    };
  }

  listItems(feedId, options) {
    if (!this.feeds.get(feedId)) return null;
    return { items: this.articles.list(feedId, options), statistics: this.articles.stats(feedId) };
  }

  listCaptures(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.captures.recent(feedId, limit);
  }

  listFetchAttempts(feedId, limit) {
    return this.refreshes.recent(feedId, limit);
  }

  feedChanges(feedId) {
    return this.changes.report(feedId);
  }

  feedHealth(feedId) {
    return this.health.report(feedId);
  }

  reviewArticle(feedId, articleId, input) {
    return this.reviewService.change(feedId, articleId, input);
  }

  reviewEvents(feedId, articleId, limit) {
    return this.reviewService.events(feedId, articleId, limit);
  }

  exportCsv(feedId, writable, options) {
    return this.csvExporter.write(feedId, writable, options);
  }

  articleTimeline(feedId, articleId, limit) {
    return this.timelines.report(feedId, articleId, limit);
  }

  auditIntegrity() {
    return this.integrityAudit.run();
  }

  close() {
    closeDatabase(this.database);
  }

  now() {
    return this.clock().toISOString();
  }
}
