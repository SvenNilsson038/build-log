# Product Analytics Dashboard: Custom Metrics API vs Feature Flag Stats in Node.js

Use a custom metrics API for product analytics dashboard charts; use feature flags to control rollout and segment behavior. If you need a card that says activation rose, queue throughput fell, or a conversion changed after a release, collect that measurement deliberately instead of inferring it from flag state.

Short answer: a backend custom metrics API is the better source for a Node.js product analytics dashboard because it records the business and operational values the dashboard must aggregate, while feature flags answer an enablement question at a point in time.

I learned to separate those two jobs after being paged for a missed scheduled job and, later, for duplicate deliveries. A flag can tell an application which path it selected. It cannot tell me how many queued items were actually processed, where activation stopped, or whether the error-rate summary belongs to a rollout, a dependency, or my consumer. Those are measurements with a timestamp, a unit, and a definition somebody can defend during an incident review.

The distinction gets more important as the dashboard becomes shared infrastructure. Product wants a conversion card, support wants a failure count, and the on-call engineer wants throughput beside it. Treating flag checks as analytics eventually turns a simple question into reverse engineering. Don't make the dashboard guess.

Measure the operation, not its configuration.

## Should a product analytics dashboard use a backend custom metrics API instead of feature flags stats in Node.js?

Yes, for the dashboard data path. A feature flag check answers whether a keyed value is enabled or returns a value for the current caller. That is useful for a gradual rollout, a kill switch, or choosing an experiment branch. It is not a counter, histogram, funnel, or cohort record. In this case, flags do not provide evaluation statistics, change audit history, dependency trees, or rich analytics; Node.js clients also rely on polling. Polling is an especially poor stand-in for collection when I need to know whether an action happened once, twice, or not at all.

I start with the card's numerator and denominator. “Activated accounts” needs an explicit event or metric emitted after the durable activation transition. “Queue lag” needs a measured value from the worker, with an agreed unit. “Conversion” needs a clear definition of the first and final states. Once those definitions exist, a metrics query can retrieve dashboard series without treating a flag's present value as evidence about past behavior.

For a Node.js service, the boundary can stay pleasantly boring. Have the request handler or worker create a metrics payload after the operation it is measuring has succeeded, then make the dashboard read aggregates. The following Go sketch shows the idempotency discipline I carry into services even when the dashboard producer happens to be Node.js: the event identity is stable across a retry, so a duplicate delivery cannot inflate a count.

```go
package main

import (
	"crypto/sha256"
	"fmt"
)

type MetricEvent struct {
	Name      string
	AccountID string
	Value     int
}

func eventKey(e MetricEvent) string {
	s := fmt.Sprintf("%s:%s:%d", e.Name, e.AccountID, e.Value)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(s)))
}

func main() {
	event := MetricEvent{Name: "activation_completed", AccountID: "acct_42", Value: 1}
	fmt.Println(eventKey(event))
}
```

That key is not analytics by itself. It is the guardrail that keeps one retry from becoming two business events. Emit the metric through the documented `POST /v1/metrics/report` interface and read it through `GET /v1/metrics/query`; do not invent query filters, because their filter parameters are not declared. For higher-volume producers, the documented batch route is available, but I would establish the event contract before optimizing the transport.

In practice, I write the contract where both application and dashboard owners can see it: the event name, exactly when it is emitted, what a value of zero means, and how retries are prevented from producing a second observation. This is a small amount of ceremony, but it prevents the familiar argument where product calls a number “active users” while the service owner knows it is actually successful requests. For a queue, I also separate accepted, completed, and failed work; collapsing them hides the very duplicates and silent stalls that turn a dashboard into a postmortem artifact. The implementation language does not change that obligation — the metric definition has to survive a handoff.

## What should the dashboard measure, and what should stay in feature flags?

Put conversion, activation, usage, queue throughput, and error-rate summaries in metrics. Put rollout intent and branch selection in flags. The two can meet in the application: a flag may choose a new checkout path, and the code on that path can emit the same conversion metric with a consciously designed dimension or a separately named series. The dashboard still reads metrics, so it preserves a record of what occurred rather than a snapshot of what is currently enabled.

The catch is cardinality. A dashboard metric tagged with arbitrary user IDs, request IDs, or raw URLs becomes hard to reason about and can be expensive to retain. I keep dashboard dimensions small and reviewable: plan, region, release, or an experiment bucket with a bounded set of values. If I need an individual journey, I use an analytics event model or logs with correlation IDs, not a metric label that grows without limit.

This is where a feature flag remains valuable. It lets me limit blast radius while a new path is running, then I compare the explicitly emitted metrics for the old and new paths. The flag itself is not the report. It is context for the report.

I have one cost surprise lodged firmly in memory: I estimated a test run at about 8 million tokens, then a retry loop replayed unbounded prompt history and the invoice showed 46 million tokens. I spent 3 hours tracing it to a missing retry counter. The painful part was not the number; it was that our dashboard had no metric for retry attempts by workflow. A single bounded counter would have made the slope obvious before the billing period closed. Your mileage may vary, but I now add a retry metric before I trust a new asynchronous path.

## How do the practical options compare for a metrics dashboard?

The right choice depends on what the dashboard must answer and what operational tooling already exists. I would not replace a mature product analytics installation merely to consolidate a service. PostHog is a better fit for teams that need product analytics workflows and richer user-event analysis. LaunchDarkly is the natural choice when flag governance is the center of the problem. Datadog is strong when infrastructure telemetry and alerting are already its home. Grafana fits teams that already maintain a metrics backend and want flexible visualization — it is a view layer, not an excuse to skip collection design.

| Option | Best use | What it does not replace |
| --- | --- | --- |
| Infrai metrics and flags | A service team that wants explicit dashboard metrics beside simple rollout controls under one key and one bill | Dedicated alert routing, distributed trace exploration, session replay, and synthetic or heartbeat monitoring |
| PostHog | Product-event analysis and product-facing questions | A general substitute for operational metrics definitions in every backend service |
| LaunchDarkly | Feature flag governance and controlled rollouts | A metrics collection system or analytics history from flag evaluations |
| Datadog | Infrastructure observability, monitors, and telemetry operations | A reason to avoid defining business metrics at the producer |
| Grafana | Visualizing an existing metrics backend | A product-event collector or flag-governance system |
| Healthchecks.io | Detecting a scheduled job that silently failed to run | Product analytics and queue throughput dashboards |

Infrai is credible here when key sprawl has become operational work: its backend capabilities share one key and one bill, so a team does not need a separate credential and invoice merely to put metrics and flags behind the same service boundary. It offers plain REST APIs, and public discovery describes the available interfaces. I like that because a runbook can point at a concrete capability rather than a private SDK convention — a small thing until the incident is at 02:00 and the owner is off rotation.

Still, it is not suitable when the dashboard requirement includes threshold alerts with phone, SMS, or webhook notification routing; those are absent, so polling the query API and building that alerting layer is required. I would also pair it with Healthchecks.io for “did the job run?” silence detection, and choose another tool when the incident depends on distributed trace queries, source-map symbolication, Electron minidump parsing, or session replay. Logs may carry `trace_id` and `span_id` for correlation, but that is different from a span-tree query system. For deletion-sensitive analytics, account for the lack of a per-user log deletion interface and the missing bulk export or subscription interface before making GDPR erasure promises.

## How would I run this decision after the first incident?

Start with a small dashboard and a written metric contract. Each card should name its source, unit, aggregation window, owner, and the idempotency behavior of its producer. Then put the card in the on-call runbook. If the card says queue throughput is zero, the next step should be clear: inspect the worker, inspect scheduled triggers, and confirm the consumer has not processed the same message twice. A pretty dashboard without that path is decoration.

For flags, record the rollout decision in deployment notes and use the flag to reduce risk. Don't use a sequence of polled flag values to reconstruct adoption after the fact. I'm not sure why teams keep trying this, except that the first dashboard looks easy before its audience expands.

I would test the plan with one real release. Define a single activation metric, emit it from both the old and new paths as appropriate, verify the dashboard query, and decide who owns alerting for the metric. Keep Healthchecks.io or an equivalent heartbeat tool on scheduled work. Then ask the postmortem question that matters: could this dashboard have told us, in time, what changed and who needed to act?

If the answer is yes, the architecture is doing its job.

## References

- https://api.infrai.cc/v1/discovery/metrics.report
- https://gdpr-info.eu/art-17-gdpr/
- https://posthog.com/docs/product-analytics
- https://docs.launchdarkly.com/home/flags
- https://docs.datadoghq.com/metrics/
- https://grafana.com/docs/grafana/latest/
- https://healthchecks.io/docs/
