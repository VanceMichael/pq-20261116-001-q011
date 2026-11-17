const WINDOW_MS = 24 * 60 * 60 * 1000;
const STALE_AFTER_MS = 6 * 60 * 60 * 1000;

export class HealthReportService {
  constructor({ feeds, fetches, syncs = null, clock = () => new Date() }) {
    this.feeds = feeds;
    this.fetches = fetches;
    this.syncs = syncs;
    this.clock = clock;
  }

  report(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) return null;
    const now = this.clock();
    const since = new Date(now.getTime() - WINDOW_MS).toISOString();
    // JSON feeds are judged by their sync/alignment runs, HTML feeds by fetch attempts.
    const summary = feed.kind === "json" && this.syncs
      ? this.syncs.summarySince(feedId, since)
      : this.fetches.summarySince(feedId, since);
    const lastSuccess = summary.lastSuccessAt ? Date.parse(summary.lastSuccessAt) : null;
    const ageMs = lastSuccess === null ? null : Math.max(0, now.getTime() - lastSuccess);
    const issues = diagnose(summary, ageMs);
    return {
      feedId,
      status: summary.total === 0 ? "awaiting_capture" : issues.length ? "attention" : "healthy",
      checkedAt: now.toISOString(),
      windowHours: 24,
      lastSuccessAt: summary.lastSuccessAt ?? null,
      lastAttemptAt: summary.lastAttemptAt ?? null,
      freshness: ageMs === null ? "unknown" : ageMs > STALE_AFTER_MS ? "stale" : "fresh",
      successRate: summary.total ? Number((summary.accepted / summary.total).toFixed(3)) : null,
      totals: {
        attempts: summary.total,
        accepted: summary.accepted,
        rejected: summary.rejected,
        failed: summary.failed,
        notModified: summary.notModified
      },
      issues
    };
  }
}

function diagnose(summary, ageMs) {
  const issues = [];
  if (!summary.total) issues.push({ code: "no_recent_attempts", severity: "warning" });
  if (summary.failed > 0) issues.push({ code: "source_failures", severity: "warning", count: summary.failed });
  if (summary.rejected > 0) issues.push({ code: "capture_rejections", severity: "warning", count: summary.rejected });
  if (ageMs === null && summary.total > 0) issues.push({ code: "no_successful_capture", severity: "critical" });
  if (ageMs !== null && ageMs > STALE_AFTER_MS) {
    issues.push({ code: "stale_source", severity: "critical", ageHours: Number((ageMs / 3_600_000).toFixed(1)) });
  }
  if (summary.total >= 4 && summary.accepted / summary.total < 0.5) {
    issues.push({ code: "low_acceptance_rate", severity: "critical", rate: Number((summary.accepted / summary.total).toFixed(3)) });
  }
  return issues;
}
