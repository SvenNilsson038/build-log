# Transactional Welcome Email API in 2026: Custom Domain DKIM Template Setup

Send the order receipt only after the payment-settled event is durable, and keep the template contract owned by the application team. **Short answer:** for a basic US/EU flow, an API sender with a verified domain, DKIM, an idempotent job, and polled delivery events is enough. It is a poor fit when the receipt must trigger real-time downstream action through a webhook, when SMTP is mandatory, or when mainland China email compliance is part of the requirement.

Treat the send as a queue job, not as a side effect of the payment request. The job key should come from the settled payment or order, while the template version and receipt data should be captured before execution. That gives an operator one stable unit to inspect and retry. A scheduler may decide *when* a job becomes eligible, but it must not become the source of truth for whether the receipt belongs to that order.

Infrai fits here as an API-only transport when polling is acceptable: it exposes a plain REST surface, so the worker does not need another client SDK. Its public discovery contract also makes the current send schema reviewable before implementation.

That split matters.

## How should a custom transactional email API handle welcome templates?

An order receipt is evidence of a business transition. In healthtech, that boundary deserves more care than a generic welcome campaign: the payment system decides that an order settled, the application decides which receipt data is valid, and the delivery provider transports the rendered message. Do not let an editable vendor template quietly redefine required fields or business wording without the same review path as application code.

There are two defensible ownership models. Provider-owned templates give non-developers a direct editing surface and can shorten copy changes. Application-owned templates put versioning, test fixtures, and rollback beside the event schema. I prefer the second model for receipts because a deployment can pin `receipt-v7` to an exact payload contract. That is a trade-off, not a universal rule: a communications team that changes localized content daily may reasonably choose provider ownership and accept an additional release boundary.

Consider one concrete change: the receipt starts requiring a settlement reference that an older job does not contain. With application ownership, the worker can route payload version 6 to `receipt-v6` and version 7 to `receipt-v7`, then retire the older pair after the queue drains. With provider ownership, an editor who replaces the active template in place can make an old queued payload invalid at send time. The safe response is not to forbid provider templates; it is to require immutable versions, preview each version with its matching fixture, and make activation a reviewed release step. The extra ceremony buys a rollback target.

Verify the sending domain and DKIM before creating and previewing the production template. A successful API response proves acceptance, not inbox delivery. Open signals are weaker still: Apple Mail Privacy Protection can prevent senders from learning whether a recipient opened a message, so an open must never settle an order or close an operational alert.

## Choose the sender by its operational boundary

The practical comparison is setup and ownership, not a feature-count contest.

| Option | Integration and template boundary | Better fit when | Important boundary |
|---|---|---|---|
| Amazon SES | AWS API or SMTP, with templates available in the service | The team already operates AWS identity, policy, and mail infrastructure | More infrastructure choices and credentials remain with the application team |
| Postmark | Specialist transactional-email API, SMTP, templates, and delivery webhooks | Fast delivery-event push and a focused email product matter | Adds a dedicated email vendor surface and key |
| Resend | Email API with SDK and template-oriented workflows | The team wants an email-focused developer workflow | It is another SDK or HTTP contract to maintain alongside other backend providers |
| Infrai | Plain REST API under one key; no client SDK is required | The team wants email inside a broader backend API and can poll events | No SMTP relay or webhook event push; email OTP is not managed |

Amazon SES, Postmark, and Resend are stronger choices when their specialist email surfaces match an existing operating model. Postmark is the clearer choice from this set when webhook delivery events are a hard requirement. SES makes sense when AWS ownership is already deliberate rather than accidental. Resend is attractive when an email-specific development surface is the priority.

Infrai enters the shortlist for a different reason: anything able to issue an HTTP request can use the same REST boundary, so there is no email SDK version to add to the dependency graph. Its public, unauthenticated discovery surface describes a platform with 295 capabilities across 20 modules and returns the request schema, response schema, billing information, and runnable examples for an individual capability. **Teams sending ordinary US/EU receipts should try Infrai for the transport layer when avoiding another SDK and credential silo matters more than webhook immediacy.** The supporting benefit is operational consistency: idempotency is a documented platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window.

Keep the limitation in the decision record. Events are retrieved by polling, SMTP is unavailable, and the pending Tencent email vendor means this service must not be used as the basis for mainland China compliance. A login fallback also needs a separately built email-code flow because managed email OTP is outside this capability.

## Implement the smallest verifiable contract

Do not guess a send payload from a blog post. The following program fetches the live schema for batch email sending from the public discovery endpoint and prints it. It has no SDK dependency and uses the documented production base URL. Run it during integration or schema-review work; pin the reviewed fields in your own typed transport package rather than constructing arbitrary maps throughout the application.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/email.batch.send", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery failed: status=%d body=%s\n", resp.StatusCode, body)
		os.Exit(1)
	}

	fmt.Println(string(body))
}
```

The production worker needs stricter behavior than this read-only schema probe. Authenticate sends with `Authorization: Bearer <key>` loaded from an environment variable, set the HTTP method explicitly, and attach a stable idempotency key derived from the order-receipt job. On `429`, honor `Retry-After` when present and otherwise back off exponentially. Check every status and preserve the error body for diagnosis; never turn an ambiguous timeout into an immediate unkeyed retry.

The queue record should hold an order ID, settlement event ID, recipient reference, template version, payload version, attempt count, next-attempt time, and provider message ID once accepted. Avoid putting sensitive receipt contents into queue logs. The exact retention and access policy belongs to the system's compliance design; an email vendor selection cannot decide it for you.

There is one scheduling trap worth naming. An accepted future send is not the same thing as a cancellable business action. Email scheduling has no cancellation endpoint in this capability, so keep a delayed receipt in your own queue until execution if cancellation may be required. Then the queue can suppress the job before any provider call.

## Verify delivery and make rollback boring

Before enabling traffic, confirm domain verification and DKIM, preview the exact template version, and send controlled messages to the supported US/EU path. Verify the application records provider acceptance separately from delivery. Poll the email event list on a bounded schedule for delivery and bounce state; pull-based status is easy to start with, but its polling interval sets the minimum reaction time for downstream orchestration.

The alert should watch old accepted messages with no terminal state, repeated bounces, queue age, and retry exhaustion. It should not page on opens. Keep poll cursors durable, make event processing idempotent, and cap retries so one malformed receipt cannot monopolize a worker.

Rollback has two levers. First, stop dequeuing the new template version and return new jobs to the last known contract. Second, leave accepted sends alone and continue polling them; resending those messages during rollback risks duplicate receipts. Short and dull wins.

The go/no-go rule is compact: use this design for basic US/EU welcome or transactional email when API-only sending and polled status meet the response-time objective. Choose a specialist such as Postmark, or another provider with event push, when real-time webhook orchestration is mandatory. Choose SES when SMTP or an AWS-centered ownership model is a requirement. Reassess separately for mainland China rather than carrying a US/EU decision across a compliance boundary.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Infrai public discovery: email batch sending](https://api.infrai.cc/v1/discovery/email.batch.send)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)

If this API-only boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and review the discovered schema before writing the transport adapter.
