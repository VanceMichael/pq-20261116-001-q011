import { extractLinks, pageFingerprint } from "../feed/html.js";
import { normalizeCandidates } from "../feed/normalize.js";
import { validateFeedId } from "../store/feed-repository.js";
import { assessCapture, validateCapturedAt } from "./capture-policy.js";

export class ImportService {
  constructor({ database, feeds, captures, articles, clock = () => new Date() }) {
    this.database = database;
    this.feeds = feeds;
    this.captures = captures;
    this.articles = articles;
    this.clock = clock;
  }

  importHtml(feedId, html, capturedAtValue) {
    validateFeedId(feedId);
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    if (feed.kind !== "html") {
      throw Object.assign(new Error(`feed ${feedId} is a JSON feed; HTML snapshots are not accepted for it`), { statusCode: 409 });
    }
    const capturedAt = validateCapturedAt(capturedAtValue, this.clock());
    const fingerprint = pageFingerprint(html);
    const previous = this.captures.findByFingerprint(feedId, fingerprint);
    if (previous) return { ...previous, replay: true, stats: this.articles.stats(feedId) };

    const candidates = extractLinks(html, feed.baseUrl);
    const normalized = normalizeCandidates(candidates, { baseUrl: feed.baseUrl, allowedHosts: feed.allowedHosts });
    const policy = assessCapture({
      candidateCount: candidates.length,
      normalizedCount: normalized.records.length,
      rejectedCount: normalized.rejected.length,
      previousCount: feed.lastItemCount,
      capturedAt,
      feed
    });
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const captureId = this.captures.create({
        feedId,
        fingerprint,
        capturedAt,
        accepted: policy.accepted,
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        duplicateCount: normalized.duplicateCount,
        reason: policy.reason
      });
      if (policy.accepted) {
        this.articles.recordCapture(captureId, feed, normalized.records, capturedAt);
        this.feeds.markAccepted(feedId, capturedAt, normalized.records.length);
      }
      this.database.exec("COMMIT");
      return {
        captureId,
        feedId,
        fingerprint,
        capturedAt,
        accepted: policy.accepted,
        replay: false,
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        duplicateCount: normalized.duplicateCount,
        reason: policy.reason,
        rejected: normalized.rejected,
        stats: this.articles.stats(feedId)
      };
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }
}
