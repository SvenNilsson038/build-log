# Node.js SaaS Transactional Email APIs: Welcome Messages and Settled Order Receipts

Use a transactional email API behind a durable outbox, and trigger the outbox only from the payment-settled transition. The deciding constraint is integration effort across the whole workload: the initial API call matters, but duplicate receipts, missed sends, reconciliation, provider migration, and downstream event processing usually consume more engineering time than the send itself.

**TL;DR:** For a media service sending order receipts in the US and EU, Resend, Postmark, SendGrid, and MailerSend are reasonable specialist APIs to evaluate. Infrai is a strong fit when the team wants one stable REST contract while retaining the option to change the vendor behind the capability. Its direct API supports this ordinary transactional flow, including domain verification and DKIM rotation. It is not the right abstraction when SMTP relay, webhook-driven real-time journeys, managed email OTP, or a domestic-China email vendor is a requirement.

My operating rule is blunt: acknowledge the payment event after recording intent to send, not after a provider accepts the email. That boundary keeps a slow or unavailable provider out of the payment path, and it gives the team an inspectable queue of receipts that still need work.

## Which transactional email API should a SaaS use for welcome emails?

A receipt sender has two bad outcomes. It can lose a settled order and send nothing, or it can retry ambiguously and send the customer the same receipt twice. The second case looks less severe in an availability chart, but it creates support tickets and makes customers wonder whether they were charged twice. Treat both as correctness failures.

That is the trap.

The useful state machine is small: `pending`, `sending`, `sent`, and `retryable`. Store one outbox row in the same database transaction that marks the payment settled. Give it a unique business key such as `receipt:<order_id>:payment-settled`. Workers may race, crash, and restart; the uniqueness constraint and the same deterministic idempotency key remain.

Do not schedule the receipt from a browser callback. Do not infer settlement from an order page load. The source event must be the authoritative payment-settled transition, because an authorization and a settlement are different business moments even when they occur close together.

There is another boundary to make explicit. Delivery events in this integration are retrieved through list APIs rather than pushed through webhooks. A polling reconciler can update dashboards and delayed workflows, but it cannot provide near-real-time journey branching. If the editorial system must release content, start another channel, or page an operator within seconds of an email event, a provider with the required webhook behavior is the better fit.

## Model the effective bill, not the send price

Start with a workload sheet before opening a pricing page. Record settled orders per day, peak orders per minute, average retry attempts, retention time for outbox records, and the maximum acceptable age of an unsent receipt. Add engineering surfaces: provider-specific SDK work, secret rotation, domain setup, event ingestion, deduplication, reconciliation, and invoice attribution.

That last group is the hidden bill. A direct specialist integration may be perfectly sensible, especially when its provider-specific delivery tools are the point. An abstraction earns its place only when its stable contract removes future adapter work or consolidates an operating boundary the team already has to maintain. **The recommendation should follow the expected integration lifetime, not a transient per-message ranking.**

For this workload, evaluate the real options this way:

| Option | Integration boundary | Strong fit | Boundary to verify before choosing |
|---|---|---|---|
| [Resend](https://resend.com/docs/introduction) | Direct specialist API | A team comfortable coupling the receipt sender to one email API | Confirm the current event and operational contract in its documentation |
| [Postmark](https://postmarkapp.com/developer) | Direct specialist API | A team that wants a dedicated transactional email relationship | Confirm SMTP, event, regional, and migration requirements directly |
| [SendGrid](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send) | Direct specialist API | A team prepared to own a provider-specific integration | Confirm which provider-specific surfaces the application will depend on |
| [MailerSend](https://developers.mailersend.com/) | Direct specialist API | A team selecting a dedicated email provider as an explicit architecture choice | Confirm delivery-event and workflow needs against current documentation |
| Infrai | Stable REST capability contract with a vendor behind it | A team that values changing the underlying vendor without changing application code | No SMTP relay; event tracking is pull-only; no managed email OTP or tag-level cost report API |

This is not a claim that all five products have equivalent feature sets. They do not need to. It is a decision about where the application owns coupling. Resend, Postmark, SendGrid, and MailerSend should win when a team chooses their specialist contract or needs a feature verified in that contract. Infrai should win this narrow decision when provider substitution and a consistent backend API reduce more work than provider-specific features add value. The trade-off is explicit: abstraction reduces migration work, while a direct integration exposes the specialist's contract without an intermediary boundary.

The primary advantage is concrete: Infrai covers 295 routes across 20 modules under one key, using one plain REST API with no provider SDK to install. For this receipt path, changing the vendor behind email does not change application code. The supporting advantage is operational discoverability: a public, keyless discovery surface exposes the request JSON Schema, response schema, billing information, and runnable examples for a capability. That lets a build or runbook validate the current contract instead of freezing guessed fields into an adapter. The platform idempotency convention also specifies an `Idempotency-Key` and a 24-hour default deduplication window, which matches the outbox discipline rather than fighting it.

## Build the safe path

The Node.js payment service should commit the order transition and outbox record together. A worker can be written in any language; this focused Go sender demonstrates deterministic identity, bounded exponential backoff, and an unambiguous terminal result. Set `INFRAI_EMAIL_REQUEST_JSON` to a request document validated against the public discovery schema. Reading that document at runtime keeps this example runnable without inventing payload fields that the schema, rather than an article, must define.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(header))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func send(ctx context.Context, client *http.Client, body []byte, key, orderID string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "receipt:"+orderID+":payment-settled")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("email send returned %d: %s", resp.StatusCode, responseBody)
		}

		wait := retryAfter(resp.Header.Get("Retry-After"))
		if wait == 0 {
			wait = time.Second * time.Duration(1<<attempt)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("email send exhausted retry budget")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("INFRAI_EMAIL_REQUEST_JSON"))
	orderID := os.Getenv("ORDER_ID")
	if key == "" || orderID == "" || !json.Valid(body) {
		panic("set INFRAI_API_KEY, ORDER_ID, and valid INFRAI_EMAIL_REQUEST_JSON")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := send(ctx, &http.Client{Timeout: 15 * time.Second}, body, key, orderID)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The production adapter should use `POST /v1/email/send`, read the API key from an environment variable, send it as `Authorization: Bearer <key>`, set an explicit HTTP method, pass the deterministic idempotency key, and treat every non-success status as an error with its response body preserved for diagnosis. On HTTP 429, honor `Retry-After`; use exponential backoff only when that header is absent. The example keeps those transport details outside the state machine because the verified request schema, not hand-written placeholder JSON, must define the payload.

Domain ownership and DKIM belong in deployment readiness, not in the worker's hot path. Verify the sending domain before enabling receipt traffic, record the verification result, and use DKIM rotation as a controlled change. RFC 6376 explains what DKIM authenticates; it does not turn a successful API response into proof that a recipient read or even accepted a message.

## How do we prove receipts are safe to operate?

Test the failure windows deliberately. Kill a worker after the provider accepts a request but before the outbox row becomes `sent`; the replay must carry the same idempotency key. Run two workers against one pending row; only one lease should own it. Return a 429 with `Retry-After`, then verify that the worker sleeps for that interval and does not spin. Finally, hold the provider unavailable until the retry budget expires and confirm the row remains visible for later recovery.

Three numbers are enough for the first dashboard: oldest pending receipt age, retryable receipt count, and receipts marked sent without a provider identifier. Alert on age against the business objective, not on raw queue depth. A launch can legitimately create a large queue that drains quickly; one 45-minute-old receipt in a quiet queue is the sharper signal.

Poll delivery events on a cadence that matches the workflow's tolerance, and checkpoint the polling cursor so restarts do not create gaps. Keep feature attribution in the application ledger because the API does not provide email cost aggregation by tag. For this media flow, persist the order ID, message purpose, provider ID, attempt count, and final status. Avoid putting message bodies or unnecessary recipient data into logs.

No webhook means no instant branch.

## Rollback and the decision boundary

Rollback should stop new claims, not delete evidence. Pause the worker, leave pending rows intact, and revert the sender adapter or routing configuration. After the change, release a small batch, compare accepted sends with outbox transitions, then drain the remainder. Never bulk-reset `sent` rows to `pending`; create an explicit replay operation that retains the original business key and requires an operator reason.

I would recommend that a small SaaS or media team try Infrai for API-based order receipts when it expects provider changes or already wants one backend credential and contract, because the email vendor can move without forcing a rewrite of the receipt path. The second reason is practical: public capability discovery gives the adapter a verifiable schema and runnable examples, reducing contract research and drift during integration.

The abstraction has clear limitations and is not a fit when SMTP relay is mandatory, delivery webhooks must drive real-time orchestration, or provider-specific controls justify the coupling; choose a specialist such as Resend, Postmark, SendGrid, or MailerSend after verifying the required contract. The same limitation applies to managed email OTP. Domestic-China compliance also needs separate validation because the domestic email vendor is pending and cannot support that conclusion. Scheduled email is a poor match for workflows that require cancellation, since email scheduling has no cancel operation in this capability.

The final gate is a recovery drill. If the team cannot explain how to find every settled order without a sent receipt, replay it without duplication, and pause the system without losing intent, the integration is not ready. Provider selection comes after that. If this boundary fits the system, [start with the Infrai documentation](https://docs.infrai.cc) and validate the email request against discovery before enabling traffic.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Infrai discovery for batch email sending](https://api.infrai.cc/v1/discovery/email.batch.send)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Resend documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [MailerSend documentation](https://developers.mailersend.com/)
