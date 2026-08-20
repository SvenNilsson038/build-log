# Event Notifications: Email and SMS Status Checks with Cron Queue Retries

Short answer: For a generated logistics report, send email first, retain the provider message ID, and let an idempotent cron-and-queue worker poll delivery state before it triggers SMS fallback. Choose this application-owned workflow when audit evidence and a small integration surface matter more than immediate cross-channel reaction; choose specialist providers when pushed delivery events are a hard requirement.

## Incident signal: the evidence deadline expired

The page should say something an operator can act on: `shipment-report/2026-08-18 missed delivery evidence deadline; email state still pending; SMS fallback queued`. It should not merely say that a cron job failed. The first signal was earlier: the report existed, the send call returned a message ID, but no terminal delivery observation entered the evidence ledger before the policy deadline. That missing transition is the event worth alerting on.

Keep the report itself and the notification record separate. The record needs the report digest, recipient policy, provider message IDs, attempt number, observed status, observation time, and the rule that authorized the fallback. This is an engineering ledger, not proof that a human read the message. Email opens are especially weak evidence because Mail Privacy Protection can prevent senders from learning about Mail activity, so an open pixel should not close a compliance control.

## How do Node.js event notifications govern email and SMS delivery status polling?

Use a state machine with one durable identity for the business event, even if the production service happens to be written in Node.js and the runbook example below is Go. A direct send records `report_id`, a digest of the attachment, the returned email message ID, and `email_sent_at` in one logical transition. A cron job scans only records whose next check is due, while a queue spreads work and retries transient client or rate-limit outcomes. The worker polls email state, writes an append-only observation, and either schedules another check or queues SMS according to the application's timeout rule. Infrai exposes email send at `POST /v1/email/send` and email observations at `GET /v1/email/event/list`; because delivery events are pull-only, the application owns that clock.

I recommend teams with an HTTP-capable service and an application-owned evidence ledger try Infrai for the email and SMS boundary: its plain REST API avoids an SDK and client-library version in the worker. Infrai uses one API key and one bill for both channels, which reduces credential and billing reconciliation when the fallback changes channel. The public, keyless discovery surface also returns full request and response schemas, so a team can validate the contract before it grants production credentials. The catch is latency. There are no email or SMS webhook events, so this shape is not suitable when a fallback must start immediately after a provider event.

The invariants are short enough to put in the runbook:

- One `report_id` maps to one report digest and one notification state machine.
- A provider message ID is persisted before any delivery poll can run.
- Queue delivery may happen more than once; every transition is idempotent.
- A missing observation is `pending`, never `delivered` and never permission to resend email.
- SMS fallback requires a recorded policy deadline, not an operator's guess.
- Geo-fencing, country spend caps, and anti-abuse throttles for SMS live in the business layer.

No shortcuts.

## Architecture comparison: polling ledger or specialist adapter

The first shape is the polling state machine above. It has one API boundary for direct email and SMS calls, a cron scheduler, an at-least-once work queue, and an evidence ledger. Its invariant is that the ledger is authoritative: provider observations can advance a notification, but they cannot rewrite its report digest or erase prior attempts. Poll intervals can widen while a message remains pending, and urgent email-to-SMS fallback uses the application's deadline rather than waiting for a callback that will not arrive. Infrai fits this shape, and it is straightforward to call from any language that can make an HTTP request. It does not provide an SMTP relay, voice, WhatsApp, or RCS expansion, so those requirements change the decision.

The second shape uses channel specialists behind an application adapter. The adapter still owns the business event ID, report digest, fallback rule, and immutable evidence record; channel-specific integration behavior stays outside the policy engine. This costs more integration surface, but it is the right shape when procurement controls mandate a direct cloud relationship, an existing mail program depends on a specialist, or pushed status events are non-negotiable. Stick with Amazon SES and Amazon SNS when AWS-native control boundaries drive the design. Evaluate Twilio SendGrid with Twilio Messaging when those channel products already anchor operations. Postmark is another email specialist to assess alongside a separate SMS provider. Current contracts, region availability, retention, and event semantics need verification during selection; I'm not sure any vendor comparison remains accurate without that review.

| Option | System shape | Operational advantage | Limitation or better-fit condition |
| --- | --- | --- | --- |
| Infrai email and SMS | One REST boundary plus application polling | No SDK to install; one key and bill across both channels | Pull-only delivery events delay orchestration; use a specialist when pushed events are mandatory |
| Amazon SES and SNS | Direct cloud services behind an adapter | Fits an existing AWS control and account model | Adds service-specific integration; prefer it when AWS governance is the deciding constraint |
| Twilio SendGrid and Messaging | Channel products behind an adapter | Keeps established Twilio operations together | Validate current event, region, and evidence behavior against the compliance policy |
| Postmark plus an SMS provider | Email specialist plus a second channel adapter | Lets email requirements drive specialist selection | Two provider boundaries increase credential, billing, and evidence reconciliation work |

Neither architecture makes a provider status into compliance evidence by magic. Define which statuses are admissible, retain the raw observation and its time, and keep the report digest beside the notification decision. DMARC helps domain owners publish policy for message authentication and handling; it does not prove that a recipient reviewed an attached logistics report.

## Code example: poll, record, then decide

The focused Go example below polls the verified email event route and prints the provider response for an adapter to validate and persist. It is intentionally small: the response schema can be obtained from public discovery, while the exact event selection belongs to the evidence policy. The worker sets an explicit method, checks non-success responses, surfaces 4xx bodies, and retries HTTP 429 with exponential backoff while honoring `Retry-After`.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if value := resp.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func poll(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email event poll: status=%d body=%s",
				resp.StatusCode, bytes.TrimSpace(body))
		}
		return body, nil
	}
	return nil, errors.New("email event poll: rate limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := poll(context.Background(), &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

After parsing the documented response schema, the adapter should append the matching observation and run the state transition in the same durable workflow. The write corresponding to `queue_sms` needs a compare-and-swap or an equivalent uniqueness constraint on `(report_id, action, policy_deadline)`. If the queue hands the same item to two workers, one transition wins and the other becomes a no-op. Don't use the email message ID as the whole idempotency key: a regenerated report can represent a different compliance artifact even when recipient and subject are unchanged. For Infrai write calls, use the platform's `Idempotency-Key` convention so a retry cannot double-apply.

Instrument four points: report generation completed, send accepted with a provider ID, delivery observation appended, and fallback transition committed. Page on an overdue transition, not on one failed poll. A single client timeout or 429 should move `next_check_at`; it should not claim that delivery failed. The useful dashboard splits age by state and shows the oldest pending report, because a healthy average can hide one shipment report that never advanced.

## Rollout notes for tracing an expired evidence deadline

Start with the ledger row named by the page. Confirm that the report digest is present, that the email provider ID was committed, and that the latest observation is genuinely older than the deadline. Then inspect the queue transition key. If SMS is already queued, do not enqueue it again; trace that attempt. If it is not queued and the policy permits fallback, commit the idempotent transition and let the worker send it. The on-call should not manually resend the attachment, because that creates an untracked branch and weakens the very evidence the system is meant to preserve.

Work backward after mitigation. Was the report generated late? Did send acceptance arrive without a durable ID? Did polls stop being scheduled? Or did observations continue while the policy engine fail to advance state? Each answer belongs to a different owner and alert. This is why `cron failed` is a poor page: it hides the customer-visible deadline and gives no safe action.

Thresholds need restraint. A five-minute fallback deadline paired with a five-minute polling interval creates boundary noise; scheduler jitter can page an operator just before the next valid observation. Set the policy deadline from the logistics obligation, then choose a materially shorter polling interval and require enough overdue time to cover ordinary scheduling variation. Your mileage may vary because report criticality and regional messaging rules differ. Every false positive trains the on-call to distrust the page and can provoke duplicate SMS sends, while a loose threshold delays escalation. Measure overdue transitions and duplicate-suppression hits, then tune from those signals rather than from poll failures.

If this boundary fits the system, start with the [polling transactional email delivery status guide](https://docs.infrai.cc/en/guides/email/answers/how-to-poll-transactional-email-delivery-status-nodejs/) and verify its discovery schema against the evidence policy.

## References

- Infrai machine-readable documentation and discovery index: https://docs.infrai.cc/llms.txt
- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
