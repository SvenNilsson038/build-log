# Feature flags in nightly pipelines: log percentage rollout and targeting for safe rollback

A nightly pipeline gives you one attempt per day, so the constraint that should drive the whole design is recovery, not evaluation speed. If a percentage rollout sends 10% of last night's media assets down a new transform and the output is wrong, flipping the flag back to 0% fixes nothing by itself — the rows are already written, and the recommendations job read them at 06:00. Pick the flag setup whose decisions you can search afterwards. Percentage rollout and basic targeting are commodity features at this point; what actually sets your rollback time is whether every unit of work carries the flag key, the variant it resolved to, and the reason it resolved that way.

Everything below is that one idea, turned into a runbook.

## Why rollback safety is the deciding axis for a batch job

A request path and a batch job disagree about what a bad flip costs. Online, the wrong variant serves some bad responses, you toggle it off, and the next request is already correct — the damage is bounded by the traffic inside the window. A nightly run persists. It wrote rows that a recommendations job, a rights-clearance report and three internal dashboards consumed this morning, and turning the flag off only changes what tonight's run does.

So the recovery procedure is never "turn it off". It's turn it off, identify exactly which assets took the new path, and reprocess only those, idempotently, before the morning consumers read again. The middle step is where incidents get long. If the only evidence you have is a line that says `applied new transcode profile` with no asset id and no variant, you're reduced to reprocessing the entire night — which is both expensive and its own source of duplicate deliveries.

The unit of work matters here. In a media catalog pipeline it's usually an asset or an episode, not a user, and that changes targeting: the subject of the flag is content, not a person, so a percentage rollout hashes an asset id and any targeting rule you write is about locale, rights window or source format.

## What should you log when a feature flag drives percentage rollout and targeting?

One structured event per evaluated unit of work, with six fields on it: run id, unit id, flag key, resolved variant, evaluation reason, ruleset version.

Put them on the record's attributes rather than baking them into a formatted message. That's the shape OpenTelemetry's log data model already assumes — a body plus typed attributes, with severity carried separately — and it's what makes the difference between `grep` and an indexed field query when you're twelve minutes into a bad morning. Severity semantics come from RFC 5424, and a routine flag decision is INFO; the fallback-to-default case is the one worth raising to WARN, because it usually means the ruleset didn't load the way you expected.

The evaluation reason is the field people skip, and it's the one that saves an argument at 07:00. Knowing that an asset got the new variant is half an answer. Knowing it got there because of the percentage split, rather than a targeting rule someone added for the EU rights window, tells you whether a rollback of the rollout is even the right lever.

| Field | Rollback question it answers |
| --- | --- |
| run.id | Which night's output is affected? |
| asset.id | Which rows do I reprocess? |
| feature_flag.key | Which change am I undoing? |
| feature_flag.variant | Did this record take the new path? |
| feature_flag.reason | Percentage split, or a targeting rule? |
| ruleset.version | Was the config stable for the whole run? |

## Pin the ruleset at run start, then emit the decision

A batch job should evaluate flags against a snapshot taken once, at the top of the run, and record the version of that snapshot on every decision. Otherwise someone bumps a rollout from 10% to 50% at 02:30 and you get a run that is neither — a mixed population that no query can cleanly describe. This is the idempotency reflex applied to configuration: the same run id plus the same ruleset version should always produce the same set of assets on the new path.

```go
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
)

// Ruleset is fetched once per run and never re-read mid-run.
type Ruleset struct {
	Version    string
	Key        string // flag key, e.g. "transcode-v2"
	Percentage int    // 0-100
	Locales    map[string]bool
}

type Decision struct {
	Variant string // "on" | "off"
	Reason  string // "TARGETING_MATCH" | "SPLIT" | "DEFAULT"
}

func (r Ruleset) Evaluate(assetID, locale string) Decision {
	if r.Locales[locale] {
		return Decision{Variant: "on", Reason: "TARGETING_MATCH"}
	}
	if bucket(r.Key, assetID) < r.Percentage {
		return Decision{Variant: "on", Reason: "SPLIT"}
	}
	return Decision{Variant: "off", Reason: "DEFAULT"}
}

// bucket is a stable 0-99 hash: same asset, same flag, same bucket, forever.
func bucket(flagKey, assetID string) int {
	sum := sha256.Sum256([]byte(flagKey + ":" + assetID))
	return int(binary.BigEndian.Uint32(sum[:4]) % 100)
}

func (w *Worker) Process(ctx context.Context, runID string, a Asset) error {
	d := w.rules.Evaluate(a.ID, a.Locale)

	log := slog.With(
		slog.String("run.id", runID),
		slog.String("asset.id", a.ID),
		slog.String("feature_flag.key", w.rules.Key),
		slog.String("feature_flag.variant", d.Variant),
		slog.String("feature_flag.reason", d.Reason),
		slog.String("ruleset.version", w.rules.Version),
	)

	if d.Variant == "on" {
		log.InfoContext(ctx, "asset routed to new transform")
		return w.transformV2(ctx, runID, a) // upsert keyed by (asset_id, run_id)
	}
	log.InfoContext(ctx, "asset routed to current transform")
	return w.transformV1(ctx, runID, a)
}
```

Two details in there are doing real work. The bucket function hashes the flag key together with the asset id, so two flags at 10% don't select the same tenth of the catalog and you don't accidentally test a stacked change. And the write is an upsert keyed by asset and run, which is what lets you replay the affected slice without producing duplicates — the same reflex you'd apply to a queue consumer.

In a typical media stack this ruleset is shared: a Next.js admin app reads the same flags through a Node.js SDK for the editorial preview, while the pipeline pulls the same ruleset over the provider's HTTP API and freezes it. Same source of truth, different lifetimes.

## Verifying the flip before you trust it, and undoing it after

Run the flag in log-only mode for one night first. Evaluate it, emit the decision events, and take the old path regardless of variant. You get a free rehearsal of the exact query you'll need under pressure, plus a check that the split is behaving:

```bash
jq -r 'select(.["run.id"]=="2026-08-11") | .["feature_flag.variant"]' run.ndjson |
  sort | uniq -c
```

At a 10% rollout over 40,000 assets you expect roughly 4,000 in the `on` bucket. If you see 40, your ruleset didn't load and everything took the default branch — which is exactly why that case deserves a WARN rather than silence.

The rollback itself is then three steps, and none of them require reasoning about what happened: set the percentage to 0, extract the affected asset ids by querying `feature_flag.variant = "on"` for that run id, and re-enqueue that list against the pinned previous ruleset version. Because the writes are upserts keyed by asset and run, replaying the slice twice costs time and nothing else. Measure the recovery, not the rollout — how long from decision to a clean re-run is the number worth putting in the runbook.

One honest gap: I'm not certain there's a good general answer for changes that also emit side effects downstream, like a notification per updated asset. Then the reprocess step isn't idempotent by itself, and you need a dedupe key that outlives the run.

## Where this shape is wrong

The catch is cardinality. One decision event per unit of work is fine at tens of thousands of assets a night, but at hundreds of millions it's a real ingestion bill, and log retention becomes the constraint that decides how far back you can roll anything. Sampling the `off` population hard while keeping every `on` decision at full fidelity is the usual compromise; the minority path is the one you'll need to enumerate.

A hosted flag service isn't required for any of this. If the nightly job is the only consumer, a versioned JSON file in object storage is the cheap alternative and it does the whole job — percentage split, basic targeting, a version string to pin — with no vendor in the recovery path. Providers in the category, from LaunchDarkly through the newer open-source entrants, earn their cost when several consumers need the same rules at once and non-engineers need to change them safely; OpenFeature is worth reading first if you want the evaluation vocabulary without committing to one of them.

Stick with the file when the pipeline stands alone. This approach is also not suitable for per-user, in-browser targeting, where evaluation latency and client-side bundle size dominate the decision and a per-decision log line per user is not something you want to pay for.

## References

- OpenTelemetry: Logs signal concepts — https://opentelemetry.io/docs/concepts/signals/logs/
- OpenTelemetry: Logs data model — https://opentelemetry.io/docs/specs/otel/logs/data-model/
- RFC 5424: The Syslog Protocol (severity semantics) — https://datatracker.ietf.org/doc/html/rfc5424
- OpenFeature specification — https://openfeature.dev/specification/
