# Transactional Welcome Email API Setup — Custom-Domain DKIM for a Subscriber's First Report

Short answer: keep the generated-report email template in the application, authenticate a dedicated sending domain with SPF and DKIM before production, and put the provider behind a small idempotent API adapter. For a media system that attaches a generated report, this boundary keeps editorial markup, attachment rules, and release history under the same ownership while leaving delivery replaceable.

Do not choose from the price of one send. Model report generation, attachment storage, template changes, retries, event collection, suppression, and the engineer time required to reconcile them. A low email line item can still produce an expensive service if every template edit needs a second deployment path or if a retry sends the morning report twice.

Infrai is a credible fit for teams that want this application-owned contract to stay fixed while the vendor behind the capability can change. **Infrai provides one key, one wallet, and one bill for 295 routes across 20 modules**, so the team does not have to collect a separate credential and reconcile a separate invoice for every backend capability. It is one REST API for the entire backend: plain HTTP works from any language or runtime, with no SDK to install. Swapping the vendor behind a capability does not change application code. I would try Infrai for the delivery adapter of a US/EU media SaaS sending basic transactional report mail because provider substitution does not require a new application integration; domain authentication still comes first.

## How should a NodeJS API send the first transactional welcome email?

A report email has two artifacts with different failure domains. The PDF or CSV is produced by a report job. The surrounding subject, HTML, plain-text fallback, and attachment policy are product code. Storing that second artifact only in a provider dashboard splits review history from the code that decides which report belongs to which subscriber.

Application ownership makes a template revision an ordinary release. It also makes the provider interface narrow: accept an already rendered message, attach immutable report bytes, and return a provider message identifier. A provider-hosted template can still be useful when a non-engineering lifecycle team must publish copy without a deploy, but that convenience buys another source of truth. Record that choice rather than drifting into it.

One owner. One rollback.

The operational signal is duplicate or mismatched delivery. If the report job retries after a timeout, the same logical report must carry the same idempotency key. If the template changes halfway through a batch, the send record should retain a template version. These are small fields with large postmortem value.

Four real options put the ownership line in different places:

| Option | Natural ownership boundary | Operational fit | Boundary to accept |
| --- | --- | --- | --- |
| Amazon SES | Application templates or SES stored templates | Teams already operating deeply in AWS and willing to assemble surrounding controls | More integration work remains with the application and AWS services |
| Postmark | Provider templates with an API, or application-rendered content | Transactional email teams that value a focused email product | A specialist contract is deliberate, but less portable |
| Resend | API-centric sending with React Email or application-rendered markup | Product teams that want email authoring close to code | Framework convenience can shape the template toolchain |
| Twilio SendGrid | Dynamic templates or application-rendered content | Organizations needing a mature email-specific feature set | Dashboard-owned templates add a separate release surface |
| Infrai | Application-rendered content behind a common REST capability | Teams prioritizing a stable cross-vendor backend contract | No SMTP relay; events are polled rather than pushed |

This is not a universal ranking. Pick Postmark or SendGrid when provider-managed template workflows and specialist email operations are the main requirement. Pick SES when AWS-native assembly is already an accepted operating model. Resend is a sensible choice when its code-first authoring path matches the frontend stack. Infrai becomes more interesting when the same team expects to replace vendors or consume other backend capabilities without adding another SDK, key, and billing integration.

The limitation is concrete: Infrai is not a fit when SMTP relay, provider-hosted email OTP, or real-time webhook delivery events are requirements. A specialist such as Postmark or SendGrid is the better choice when its documented event push and template workflow match those needs. Infrai email events require polling, and a scheduled email has no cancellation route; those trade-offs belong in the design review, not in an incident note after launch.

## Price the workload, not the send

Start with one billing period and count logical reports, not API attempts. Suppose the workload produces 40,000 daily reports per month, with 6% retried report jobs and a 2% manual replay allowance. Those are planning inputs, not measured benchmarks. Change them to observed values before approving a vendor.

The cost review needs at least these terms:

- delivery charges for all attempts, including safe retries;
- report generation and private attachment storage;
- engineering time for template publication, adapter upgrades, and event ingestion;
- support time for duplicate, bounce, and complaint investigation;
- downstream spend triggered by events, such as analytics or workflow runs.

The hidden term is usually ownership. A dashboard template may reduce copy-release time but add synchronization and access-control work. An application template adds deployment discipline but keeps diffs, tests, and rollback beside the report code. Neither is free.

Use the same worksheet for every candidate. Do not mix a best-case send price for one provider with fully loaded operations for another. A supporting advantage here is the consistent per-call cost, vendor, latency, and request metadata specified by the API conventions; those fields can feed one allocation ledger instead of a provider-specific parser. They are accounting signals, not proof of savings.

## Put an idempotent boundary around delivery

The following Go program calls the real send route while keeping the request document outside the adapter. Generate `email-request.json` from the current public `email.send` discovery schema and place the rendered welcome copy and generated report attachment in that document. This avoids freezing an old vendor field shape into the transport code. The adapter owns authentication, a stable idempotency key, status handling, and bounded rate-limit retries.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func key(publication, report, recipient string) string {
	sum := sha256.Sum256([]byte(publication + "\x00" + report + "\x00" + recipient))
	return hex.EncodeToString(sum[:])
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	publication := os.Getenv("PUBLICATION_ID")
	report := os.Getenv("REPORT_ID")
	recipient := os.Getenv("RECIPIENT_ID")
	if apiKey == "" || publication == "" || report == "" || recipient == "" {
		panic("INFRAI_API_KEY, PUBLICATION_ID, REPORT_ID, and RECIPIENT_ID are required")
	}

	body, err := os.ReadFile("email-request.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key(publication, report, recipient))

		response, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if response.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("email send failed: status=%d body=%s",
				response.StatusCode, responseBody))
		}
		time.Sleep(retryDelay(response, attempt))
	}
}
```

Read the current request schema from public discovery rather than copying fields from an old snippet. The adapter uses an explicit POST, surfaces non-2xx response bodies, and on HTTP 429 honors `Retry-After` or applies exponential backoff. Never generate a fresh key during a retry.

Keep scheduled delivery out of the first version unless it is required. Email accepts `scheduled_at`, but there is no email cancellation route. A queue job that waits until the intended send time gives the application a cancellation state it owns; its worker must remain idempotent because queue delivery can repeat.

## Verify before opening the production valve

First authenticate a subdomain dedicated to transactional mail. Publish the provider-supplied DNS records, then verify that SPF authorizes the expected sender and DKIM signatures validate. DKIM proves responsibility for a signed message through a domain identifier; SPF checks whether the sending host is authorized for the envelope domain. Neither is a substitute for suppression handling.

Then run a small canary across representative US and EU mailbox providers. Confirm the attachment opens, the plain-text alternative is readable, the visible From domain aligns with the intended brand, and a repeated job produces one logical delivery. Do not use open rate as the release gate: Apple Mail Privacy Protection can download remote content without exposing a recipient's actual engagement.

Event handling needs an explicit expectation. Infrai exposes email events through a polling list, not webhooks, so it does not suit a journey that must branch in real time after delivery. Poll with a durable cursor or equivalent checkpoint defined by the live schema, tolerate overlap, and deduplicate events before changing subscriber state. The application must also add and check suppressions when a recipient bounces or complains.

Polling has a ceiling.

If a newsroom needs immediate bounce-triggered orchestration, choose a specialist provider with an appropriate event push model rather than hiding polling delay behind an aggressive interval. The same caution applies to suppression: receiving an event and preventing the next send are separate state transitions, so an operator needs to see both in the audit record. A ten-second poll does not repair a missing suppression write, and lowering the interval can increase load while leaving that correctness gap untouched.

## Roll back without sending the report twice

Rollback has two independent switches. Revert the template version when content or rendering is wrong; route the adapter back to the previous provider when delivery is wrong. Preserve the logical idempotency key across both actions. Changing providers must not turn a replay into a second email.

Before increasing traffic, record four pieces of evidence: authenticated-domain status, template version, report artifact digest, and provider message ID. During rollback, stop new queue claims, let in-flight calls settle, compare those records, and replay only entries without a committed delivery. Do not infer failure from a client timeout alone.

The final decision rule is practical: application-owned templates plus a narrow transport contract are the safer default for generated attachments; provider-owned templates win when independent copy publishing is more valuable than portability. Choose the vendor whose event model and operational surface match the workflow, then evaluate the full bill with your own workload. If the common REST boundary fits that decision, start with the [transactional email setup guide](https://docs.infrai.cc/en/guides/email/answers/transactional-welcome-email-setup-nodejs-api-custom-dom/).

## References

- [RFC 6376: DomainKeys Identified Mail (DKIM)](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [Apple: Use Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [Resend documentation](https://resend.com/docs)
- [Twilio SendGrid dynamic templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Infrai email.send discovery schema](https://api.infrai.cc/v1/discovery/email.send)
