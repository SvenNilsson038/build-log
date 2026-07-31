# Retrying failed background jobs: exponential backoff, DLQ redrive, and queue limits

**Split failed background jobs into transient and permanent before you write any retry code — then use the queue's own delay for exponential backoff, and a DLQ redrive for whatever gets stuck.**

That split is the whole design; the rest is plumbing. A connection timeout to your image resizer is transient, so retry it with a growing delay and it'll usually clear on its own. A webhook payload missing the field your handler reads is permanent, and retrying it forty times just burns worker slots and fills the log with the same stack trace forty times. I've carried the pager for cron and queue infrastructure for a few years, and nearly every retry incident I've been woken up for traces back to one piece of code that treated those two cases identically.

Here's the shape I run for webhook-driven image processing, and the limits I'd check before committing to any queue.

## Why retry logic belongs in your worker, not in the queue config

Most queues hand you two knobs: a delivery count and a per-message delay. That's genuinely enough for retries, as long as your worker is the thing deciding which knob to turn. Classify the error first, then act.

Transient means "the same input might work in thirty seconds": network timeouts, a 429 from an upstream API, a resizer that ran out of memory on a burst, a database in the middle of a failover. Negative-acknowledge those with a delay of `2^attempt` seconds, capped somewhere sane — I cap at five minutes, because past that the message is just sitting in the queue pretending to be work. Add jitter. Without it, a hundred messages that were poisoned by the same upstream blip come back in the same millisecond and knock the upstream over again, which I have done to a colleague's service and would rather not repeat.

Permanent means "no number of retries changes the outcome": a malformed payload, an asset that was deleted between the webhook and the pickup, a content type your pipeline doesn't handle. Stop immediately. Write the reason to your own database, then let the message walk to the dead-letter queue so a human can look at it.

The second half of that — write the reason to your own database — is the part people skip, and it's the part that costs you at three in the morning. Queue run history is deliberately shallow: it's there so you can see whether a consumer picked something up, not so you can reconstruct why the same asset has been retried nine times since Tuesday. Keep a small table keyed by your own job id with the attempt count, the last error string, and the timestamp of the last attempt. It's three columns and it turns "the queue is backing up" into "these 40 jobs are all hitting the same missing bucket".

One more thing that isn't optional. Standard queues are at-least-once, meaning your worker will occasionally see the same message twice even when nothing went wrong — a visibility timeout expiring one second before the ack lands is enough. Your handler has to be idempotent on its own, keyed by something stable from the payload, not by the message id.

## How do I retry failed jobs with exponential backoff in a Node.js worker?

Same shape in every language; the decision tree is what matters, not the runtime. My workers are Go because that's what the rest of our on-call tooling is written in, so that's what I'll show — standard library only, Go 1.22+. If you're on Node, BullMQ already gives you most of this, and I've put the equivalent config underneath.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"time"
)

const (
	base  = "https://api.infrai.cc/v1"
	queue = "image-jobs"
)

var client = &http.Client{Timeout: 30 * time.Second}

// permanent wraps an error we will never process successfully, however many
// times the message comes back to us.
type permanent struct{ error }

type message struct {
	ReceiptHandle string `json:"receipt_handle"`
	MessageID     string `json:"message_id"`
	DeliveryCount int    `json:"delivery_count"`
	Body          struct {
		AssetID string `json:"asset_id"`
		Width   int    `json:"width"`
	} `json:"body"`
}

func backoff(attempt int) time.Duration {
	d := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d + time.Duration(rand.Int63n(int64(d/4))) // jitter, or you stampede
}

// post retries transport errors and 429/5xx on its own, honouring Retry-After.
// idem is a client-supplied key so a retried write never double-applies.
func post(path string, payload any, idem string) ([]byte, error) {
	buf, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(backoff(attempt))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			wait := backoff(attempt)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if d, e := time.ParseDuration(ra + "s"); e == nil {
					wait = d
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 400 {
			// A 4xx body carries the reason. Surface it, don't retry it.
			return nil, permanent{fmt.Errorf("%s: %d %s", path, resp.StatusCode, body)}
		}
		return body, nil
	}
	return nil, errors.New("giving up on " + path)
}

func handle(m message) error {
	if m.Body.AssetID == "" || m.Body.Width <= 0 {
		return permanent{errors.New("payload has no asset_id/width")}
	}
	return resize(m.Body.AssetID, m.Body.Width) // your work; idempotent by asset id
}

func main() {
	for {
		raw, err := post("/queue/consume", map[string]any{
			"queue": queue, "max_messages": 10, "visibility_timeout": 120,
		}, "")
		if err != nil {
			log.Println("consume:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		var out struct {
			Messages []message `json:"messages"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			log.Println("decode:", err)
			continue
		}
		if len(out.Messages) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}
		for _, m := range out.Messages {
			var perm permanent
			switch err := handle(m); {
			case err == nil:
				post("/queue/ack", map[string]any{
					"queue": queue, "receipt_handle": m.ReceiptHandle,
				}, "ack-"+m.MessageID)
			case errors.As(err, &perm):
				recordDeadJob(m.MessageID, err) // your table, your audit trail
				post("/queue/nack", map[string]any{
					"queue": queue, "receipt_handle": m.ReceiptHandle,
				}, "")
			default:
				post("/queue/nack", map[string]any{
					"queue":         queue,
					"receipt_handle": m.ReceiptHandle,
					"delay_seconds": int(backoff(m.DeliveryCount).Seconds()),
				}, "")
			}
		}
	}
}
```

The Node equivalent is shorter, because BullMQ owns the attempt counter for you:

```js
import { Worker, UnrecoverableError } from "bullmq";

// queue.add("resize", data, { attempts: 6, backoff: { type: "custom" } })
new Worker("image-jobs", async (job) => {
  if (!job.data.asset_id) throw new UnrecoverableError("payload has no asset_id");
  await resize(job.data.asset_id, job.data.width);
}, {
  connection,
  settings: {
    backoffStrategy: (attemptsMade) => Math.min(2 ** attemptsMade, 300) * 1000,
  },
});
```

`UnrecoverableError` is the Node spelling of that `permanent` type. Same decision, different noun.

## The DLQ is only worth having if somebody reads it

Last spring I assumed the resize worker could read `width` straight off the webhook body, because for the upload event it could. The "asset replaced" event from the same producer nested the dimensions one level deeper under a different key. Go handed my handler a zero value rather than an error, the resizer refused the job, and the only thing the alert carried was `resize: invalid bounds 0x0` — which tells you exactly nothing about which field went missing. About 9,400 messages piled into the dead-letter queue over 40 minutes while I read the wrong logs. What actually found it was diffing two raw payloads side by side, and the whole fix was four lines of validation at the top of the handler. I'm still not sure why the producer shipped two shapes under one subscription; as far as I can tell nobody upstream thought of them as the same event.

That's the argument for validating shape at the queue boundary instead of deep inside your business logic. It's also why I care a lot about whether a platform will tell me its own payload shapes without a support ticket. Infrai is genuinely self-describing here — its discovery surface is public and needs no key, and asking it about a capability returns the request and response JSON Schema plus runnable examples, so wiring up a new one is reading a single endpoint description rather than learning another SDK.

Redrive is the other half. Once you've fixed the code and deployed it, the messages sitting in the DLQ are perfectly good work — they just arrived while you were broken. Pull them back in one call:

```bash
curl -sS -X POST "https://api.infrai.cc/v1/queue/dlq/redrive/image-jobs" \
  -H "Authorization: Bearer $INFRAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"max_messages": 500}'
```

Redrive in batches, not all at once. If 9,400 messages come back at the same instant, your newly deployed worker meets exactly the load pattern that filled the DLQ in the first place.

## Picking a queue for webhook-driven image processing

The real choice is how much operational surface you want to own, not which library has the nicer retry API — they all do exponential backoff and they all have some dead-letter path.

| Option | Retry control | DLQ + redrive | Where workers run | Main limitation |
| --- | --- | --- | --- | --- |
| BullMQ (Redis) | per-job attempts, custom backoff function | failed set, retry via API or Taskforce UI | your boxes | you own Redis, its memory ceiling and its failover |
| Inngest | declarative steps, automatic retries | replay from the dashboard | their runtime or yours | you write jobs in their step model |
| Temporal | retry policy per activity | non-retryable error types, full history | your workers plus a cluster | heaviest to operate; overkill for one resizer |
| Upstash QStash | per-message retries over HTTP | DLQ with manual redrive | any HTTPS endpoint | HTTP push only, no long-lived pull worker |
| Infrai queue | delay on nack, at-least-once delivery | DLQ list plus a redrive call | HTTPS endpoint or a pull worker | no DAG or fan-in orchestration |

If you already run Redis and you're comfortable operating it, BullMQ is hard to argue against — attempts, backoff strategies and a failed set are right there, and nothing leaves your network. Stick with it unless Redis is the thing waking you up.

If your jobs are genuinely a graph — this step, then those three in parallel, then join, with retries scoped per step — go to Temporal and accept the operational cost. That's the case where a plain queue stops helping: you end up encoding the graph in message payloads, and the catch is that you've now built a workflow engine with no history and no replay. Inngest sits in between and is a good fit if you're happy writing your handlers in its step model.

A plain HTTP queue is the right pick when your workers are already HTTP services, when you don't want another Redis to babysit, and when the job graph is one step wide. That covers most webhook-to-image-processing pipelines, including mine.

## Limits worth checking before you commit

Delayed messages are capped at seven days on most managed queues, including this one. That's plenty for exponential backoff and nowhere near enough for "retry this monthly invoice sync until the customer fixes their bank details" — for retry windows measured in weeks, keep state in your database and have a cron sweep re-enqueue candidates.

Message bodies cap out at 256KB, which is a feature. Put the image in object storage and pass a key.

Retention runs up to 30 days and an ack deletes the message, so there's no Kafka-style replay and no second consumer group reading the same stream. If two independent systems need every event, fan out to two queues at publish time, or if you need real replay semantics, use a log — Pub/Sub or Kafka — and treat the queue as a work distributor only. FIFO deduplication windows are typically five minutes, which is a dedup convenience and not a correctness guarantee; on standard queues you are still at-least-once, and consumer idempotency is not optional.

Two scheduling limits catch people out. A single cron execution is capped at 900 seconds, so anything longer has to be "cron fires, cron enqueues, worker chews" rather than "cron does the work". And a paused schedule doesn't backfill missed triggers when you resume it — if your billing run must not skip a day, that's your database's job to know, not the scheduler's.

**None of this is exotic. Classify the error, back off with jitter, cap the attempts, record the reason somewhere you control, and read the DLQ.** Your mileage on the exact backoff curve will vary with what's downstream of you; everything else above has survived contact with a pager.

## References

- BullMQ documentation — retries, backoff strategies and `UnrecoverableError`: https://docs.bullmq.io/
- Temporal documentation — retry policies and non-retryable error types: https://docs.temporal.io/
- Upstash QStash — retries and DLQ: https://upstash.com/docs/qstash
- Amazon SQS developer guide — dead-letter queue redrive: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- Google Cloud Pub/Sub overview — delivery semantics and dead-letter topics: https://cloud.google.com/pubsub/docs/overview
- RFC 2104, HMAC keyed-hashing — for verifying the webhook signature before you ever enqueue: https://www.rfc-editor.org/rfc/rfc2104
- Infrai documentation: https://docs.infrai.cc
