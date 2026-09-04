# Nightly Reconciliation: Node.js Background Jobs, Max Attempts, Poison Messages

Short answer: stop a retry loop by making the reconciliation job finite and inspectable. Persist a stable job ID, classify the failure, enforce a maximum attempt count, and move an unchanged failure to a dead-letter queue (DLQ). Redrive only after something about the input, dependency, or handler has changed.

I've been paged by missed jobs and duplicate deliveries. For a B2B SaaS payment reconciliation run, the useful question is not whether a worker threw an exception. It is whether the next delivery has a credible chance of producing a different result.

Stop the loop.

## What belongs in a recoverable background job record?

Treat the job record as part of the recovery mechanism, not as an afterthought in application logs. For each bounded account and reconciliation date, keep a stable job ID, payload checksum, attempt number, error class, last error, handler version, dependency request ID, and idempotency key. The queue's delivery counter helps operators, but it should not be the sole history: process restarts and redrive can make broker-level delivery counts hard to interpret.

The job should also state what “done” means. A database constraint or equivalent atomic guard on `(account, reconciliation_date)` prevents a second delivery from applying the same ledger result twice. Only then should the worker acknowledge the message. An acknowledgement can be lost after a successful business write; at-least-once delivery will quite reasonably make the item visible again.

For a nightly schedule, keep the time window in the job itself. A scheduler retry must reproduce the same window and ID rather than create another open-ended run. The worker can then answer three separate questions: was this window started, was the provider data fetched, and was the local result committed? That separation turns a vague “reconciliation failed” alert into a place to resume or quarantine.

## How can a background job queue stop retries for poison messages?

Classify before requeueing. A timeout, connection reset, or rate limit is normally retryable. A malformed payment identifier, an invalid schema value, or a deterministic business-rule rejection is a poison candidate. Unknown errors get a short retry budget while an engineer examines the evidence. A permanent error should go to the DLQ without spending the remaining budget.

The failure path should be boring enough to test as a state machine:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Job struct {
	ID      string
	Date    string
	Attempt int
}

type Decision string

const (
	Retry       Decision = "retry"
	Quarantine  Decision = "quarantine"
	Acknowledge Decision = "acknowledge"
)

var ErrPermanent = errors.New("permanent input error")

func process(ctx context.Context, job Job, reconcile func(context.Context, Job) error) (Decision, error) {
	err := reconcile(ctx, job)
	if err == nil {
		return Acknowledge, nil
	}
	if errors.Is(err, ErrPermanent) || job.Attempt >= 5 {
		return Quarantine, err
	}

	// The queue applies the delay; the handler does not spin in memory.
	delay := time.Duration(1<<job.Attempt) * time.Second
	return Retry, fmt.Errorf("retry after %s: %w", delay, err)
}
```

The adapter maps these decisions to its broker's acknowledge, reject, and delayed-redelivery operations. The example's five attempts are a policy placeholder, not a universal answer. Set the limit from the reconciliation deadline, provider behavior, and cost of duplicate work. Exponential backoff with jitter reduces synchronized pressure; it cannot repair an invalid payload.

One bounded failure path is enough. A second queue should not quietly reset the count. On redrive, retain the original ID, add a redrive batch ID, and release a small, observable batch. If the same payload returns to the DLQ with the same error, quarantine it again instead of expanding the loop.

## What should an on-call verify before DLQ redrive?

Start with business lag: is yesterday's ledger complete, and which account windows are open? Then inspect queue age, retry counts by error class, DLQ arrival rate, duplicate-write rejections, and the handler version. A quiet primary queue can hide a serious failure if all the work has moved to the DLQ.

Here is the failure sequence worth rehearsing in a staging run. The scheduler creates one job for a fixed date. The worker validates the payload before calling the payment provider. A temporary provider timeout receives delayed retries, while an invalid account reference is quarantined immediately. If the worker dies after committing the ledger row but before acknowledging the message, the same ID is delivered again; the idempotency guard returns the existing result, and the duplicate delivery is recorded rather than applied twice. An operator can then compare the original error, handler version, and input checksum before deciding whether a redrive is meaningful. That is a recovery test with observable checkpoints, not a test that merely counts how many times a handler throws.

Measure both transport and outcome. Alert on reconciliation completion lag, oldest message age, DLQ growth, and duplicate-write rejection rate. A postmortem should end with a changed condition: validation moved earlier, a dependency error received an explicit class, or the scheduler stopped creating duplicate windows. “We increased retries” is not a recovery plan.

This model is not suitable when the workload needs unbounded event replay, several independent consumer groups, or long-running workflow coordination. Use a log-based stream or workflow orchestrator when those are primary requirements. The trade-off is clear: a bounded queue and DLQ make completion and ownership easier to reason about, but they give up replay flexibility. Choose the other model when replay is the product requirement, not an incident response technique.

## References

- https://en.wikipedia.org/wiki/Cron
- https://en.wikipedia.org/wiki/Exponential_backoff
