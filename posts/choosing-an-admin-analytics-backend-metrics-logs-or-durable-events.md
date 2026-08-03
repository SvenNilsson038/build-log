# Choosing an Admin Analytics Backend: Metrics, Logs, or Durable Events?

Use a metrics dashboard when admin analytics asks repeated aggregate questions, otherwise reach for logs search when an operator needs to reconstruct one event. For exact customer-facing totals in a Node.js SaaS, use neither as the source of truth: write durable business events to a database or warehouse, then treat metrics and logs as operational projections.

I run cron and queue infrastructure in production. A pretty panel matters less to me than proving that a scheduled job ran once, a retry didn't duplicate a write, and an on-call engineer can move from a bad rate to the affected operation. The easiest backend is the one with a narrow job and a documented failure mode.

## Should a Node.js SaaS admin analytics backend use a metrics dashboard or logs search?

Classify the question before choosing the interface. A metrics dashboard fits bounded questions that recur: completion rate, queue age, error ratio, or duration by operation and outcome. Those dimensions can be deliberately limited, so the same measurement supports a panel and an alert. A dashboard is the quickest read when I need to know whether the service is drifting away from normal.

Logs search fits questions with an identity: which tenant saw a rejection, which request carried a bad field, or which worker attempted a job. A structured record can preserve a correlation ID, job ID, and outcome for investigation. The catch is that flexible fields still require decisions about retention, access, privacy, indexing, and query load. A search box hides none of that.

Admin analytics can also mean exact counts by customer, plan, invoice, feature, or closed billing period. That is durable business data. Metrics may aggregate away detail, while logs can age out or be filtered. Trace sampling is explicitly a choice about which traces are recorded and exported; head sampling decides early, while tail sampling waits for more trace information. A sampled trace stream is therefore the wrong ledger for an exact total.

My operating rule is simple: **metrics for trends and alerts, structured logs for diagnosis, durable events for product truth.** This split adds one projection boundary, but it prevents an observability retention change from silently changing an admin report. It also makes reconciliation possible: for a closed interval, compare the operational count with committed business events and investigate the gap rather than pretending the two stores have identical semantics.

Keep it boring.

## Design the signals around failure, not the screen

Start at the completion boundary, after the operation's outcome is known. Emit one low-cardinality metric update for the aggregate view and one structured log record for the diagnostic view. Don't use tenant IDs, request IDs, or job IDs as metric dimensions; their value sets grow with traffic. Put those identifiers in logs, where they act as lookup keys, and keep the metric dimensions to a reviewed allowlist such as operation and outcome.

| Backend role | Best question | Data shape | Failure to plan for |
|---|---|---|---|
| Metrics dashboard | Is behavior changing over time? | Aggregates with bounded dimensions | A missing series can resemble zero activity |
| Logs search | What happened to this operation? | Structured events with correlation fields | Retention or access policy can remove investigative context |
| Durable event store | What exactly happened for the customer? | Transactional business records | A retry can duplicate a write without an idempotency constraint |

Scheduled work needs two timestamps: scheduled time and actual start time. Without both, queue delay and execution duration collapse into one number and the dashboard tells the wrong story. For queues, I graph age beside completion rate. A flat completion rate can look harmless until age rises and shows that work is arriving faster than it clears. I once handled a duplicate-write page where a naive retry ran the same operation twice. The first attempt committed, its response disappeared, and the retry inserted record number 2; tracing the shared job ID took me 47 minutes. We first saw a rate change, then followed the job ID through both attempts, and finally checked the database constraint that should have defined the operation's identity. A durable uniqueness key stopped the next retry at the write boundary. The metric exposed the symptom and the logs reconstructed the attempts, but neither could enforce idempotency after the fact — that belongs in the business write path. Privacy belongs in this same design review. I use an allowlist for telemetry fields and exclude tokens, payload bodies, customer email, and other sensitive values. Correlation IDs should be generated at ingress and carried through asynchronous work. As far as I can tell, the exact retention and access model that works best depends heavily on the team's incident obligations; your mileage may vary.

Count once.

## Implement one completion boundary safely

The service runtime doesn't change the architecture. In Node.js I would connect this boundary to the team's selected metrics and structured-logging libraries; the Go example below uses the standard library because my production runbooks favor small, inspectable examples. It validates the outcome, increments one bounded counter, and writes identifiers only to the structured diagnostic record.

```go
package main

import (
	"encoding/json"
	"expvar"
	"log/slog"
	"net/http"
	"os"
	"time"
)

var completed = expvar.NewMap("operations_completed_total")

type completion struct {
	Operation  string `json:"operation"`
	Outcome    string `json:"outcome"`
	JobID      string `json:"job_id"`
	DurationMS int64  `json:"duration_ms"`
}

func recordCompletion(w http.ResponseWriter, r *http.Request) {
	var event completion
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if event.Outcome != "ok" && event.Outcome != "error" {
		http.Error(w, "outcome must be ok or error", http.StatusBadRequest)
		return
	}

	completed.Add(event.Operation+":"+event.Outcome, 1)
	slog.InfoContext(r.Context(), "operation completed",
		"operation", event.Operation,
		"outcome", event.Outcome,
		"job_id", event.JobID,
		"duration_ms", event.DurationMS,
		"observed_at", time.Now().UTC(),
	)
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	http.HandleFunc("POST /complete", recordCompletion)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
```

There is an important limit here: `operation` must come from a fixed registry, not directly from a URL, customer value, or user input. Otherwise this compact example can still create an unbounded set of counter keys. In the real write path, commit the durable event and its idempotency key transactionally before this completion signal is emitted. Telemetry must observe the result; it must not decide whether the business transaction succeeded.

Don't make application availability depend on the analytics view. Bound telemetry queues and memory, define what happens under backpressure, and keep the business commit independent of an exporter. The exact delivery policy is a trade-off: dropping an operational signal may be acceptable under a declared limit, while dropping a billing event isn't. This is why a single "analytics backend" is not suitable when the page contains both operational trends and auditable customer totals.

## Verify deployment, absence, and rollback

Roll the new signals out behind a configuration flag. In staging, submit one successful operation and one rejected operation, then confirm that each counter changes at the intended boundary and each structured record carries the expected correlation fields. Next, simulate a client timeout after the durable commit and retry with the same idempotency key. The business record must remain singular even though the diagnostic trail contains multiple attempts.

Check absence as carefully as presence — a failure counter at zero can mean no failures, no traffic, a broken query, or a stopped exporter. Cron paths need an expected-work check or heartbeat because "nothing failed" and "nothing ran" otherwise look identical. Queue runbooks should pair completion with age and define an owner for each alert.

Before production rollout, I reconcile the new aggregate against the durable event count over the same closed interval. I expect documented differences for rejected work or intentionally sampled diagnostic data, but unexplained drift blocks the deployment. I'm not sure why this check is often deferred; it catches double counting at enqueue and completion faster than staring at either dashboard alone.

**Rollback must leave the business write path untouched.** Disable the new signal if its dimensions escape the allowlist, telemetry volume crosses the agreed budget, or a field violates the data policy. Revert the panel and alert definitions with the same change, restore the previous alert route, and exercise that route in a controlled test. The runbook should name the signal owner, retention policy, field allowlist, reconciliation query, configuration flag, and rollback condition.

Choose a managed observability backend when the team cannot own storage, indexing, upgrades, and on-call care for the telemetry system. Choose a self-managed backend when infrastructure control is required and that operational capacity already exists. Neither choice should own exact admin truth. The durable event store does that job; the dashboard reports the trend, and logs explain the exception.

## References

- OpenTelemetry, "Sampling": https://opentelemetry.io/docs/concepts/sampling/
