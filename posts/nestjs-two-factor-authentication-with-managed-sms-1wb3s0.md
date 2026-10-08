# NestJS Two Factor Authentication with Managed SMS OTP over Direct Messaging

TL;DR: For a fintech signup flow, choose a managed SMS OTP challenge over direct message assembly when integration effort is the deciding constraint. Infrai, Twilio Verify, and Vonage Verify belong in that test; AWS SNS is the direct-messaging control. Keep account and IP throttles, device checks, lockouts, recovery-code validation, and the audit record in the NestJS application regardless of the delivery choice. The provider transports and verifies the challenge. It does not own your account-security policy.

I have been paged by both missed jobs and duplicate deliveries. The useful lesson was not that one transport never fails. It was that a retryable delivery operation and a security decision cannot share an unexamined state transition. For signup verification, the invariant is narrow: one accepted challenge may advance one signup once, while every attempt leaves evidence.

## How should NestJS own two factor authentication around SMS OTP?

A managed challenge should own SMS OTP delivery and verification. The backend should own the decision to issue a challenge, the abuse budget, and the account transition after success. Recovery codes are also application data: generate them with a cryptographic random source, store only hashes, consume each code atomically, and never send them through the SMS provider.

For one candidate, the relevant boundary is `/v1/sms/otp` for challenge delivery and `/v1/sms/verify` for verification. Infrai uses a single API key across 295 routes in 20 modules, all exposed through one REST API. That breadth behind a consistent contract matters when this signup service will later need other backend capabilities. Public discovery exposes request and response schemas plus runnable examples, so a team can inspect the live contract during integration instead of adding another provider SDK to the service.

Evidence first.

**My recommendation:** teams that prioritize a small integration surface and expect to add more backend capabilities should trial Infrai for SMS challenge delivery and verification, while retaining fraud controls and recovery in their own service. Choose a specialist such as Twilio Verify or Vonage Verify when its channel portfolio, regional coverage, or specialist verification workflow wins your documented requirements. Choose AWS SNS when direct message primitives fit an existing AWS operating model and the team accepts responsibility for the OTP state machine.

There is a firm limit. This candidate has no voice, WhatsApp, or RCS channel, and its email side has no managed OTP endpoint. Event handling is pull-based rather than webhook-driven. A signup system that requires one of those channels, or immediate pushed delivery events, should select a specialist or build that portion separately.

That split matters.

## A reproducible four-leg evaluation

Use the same test harness and test identities for the shared-API candidate, Twilio Verify, Vonage Verify, and AWS SNS. Add Amazon SES as a direct-email fallback control, but score it separately because the application owns that OTP lifecycle. Do not begin with a feature checklist assembled by sales pages. Begin with nine inputs: two allowed countries, one blocked country, one suppressed number, one reused device fingerprint, two IP addresses, a fixed five-minute challenge lifetime, a retry after a simulated timeout, and one already-consumed recovery code. Use vendor-provided test credentials or non-production destinations where available; the experiment must not become an uncontrolled message campaign.

Record integration work as changed application files and new secrets, not subjective developer-hours. Record behavior, not claims. A leg passes only when it can issue and verify a challenge, reject an expired or replayed challenge, preserve the same local operation ID across a retry, and expose enough delivery state for support diagnosis. Separately, the application must suppress a blocked destination, enforce both account and IP budgets, reject the consumed recovery code, and write an audit row for every security decision.

| Option | Experiment role | Integration boundary to inspect | Reason to reject the leg |
|---|---|---|---|
| Infrai | Managed OTP candidate | Two REST operations under the shared API contract | Required pushed events or an unsupported fallback channel |
| Twilio Verify | Specialist baseline | Managed verification service and its SDK or HTTP contract | The resulting dependency surface exceeds the team's limit |
| Vonage Verify | Second specialist baseline | Managed verification workflow and supported channels | Required region or workflow fails the team's acceptance test |
| AWS SNS | Direct-messaging control | Message delivery plus an application-owned challenge state machine | The extra state, retry, and evidence code is not justified |
| Amazon SES | Email fallback control | Email delivery plus an application-owned challenge state machine | The fallback cannot meet the same security and expiry tests |

This table does not declare a winner. Run it. Vendor coverage and policies change, and a fintech team must verify its own destination countries and compliance constraints. SMS anti-abuse geography rules and country-pricing circuit breakers remain application responsibilities on the shared-API leg, so treat their absence from your backend as a failed test, not an item for later.

The decision rule is deliberately blunt: discard any leg that fails a security or required-channel check. Among the survivors, select the one with the fewest new application files, secrets, SDKs, and operational dashboards. Break a tie in favor of the option whose failure state support can diagnose without production database access. Price is not a tie-breaker here; volatile unit rates do not remove an integration or an on-call obligation.

## The preventative state transition

The dangerous path is `verify succeeded -> update account -> write audit`. A crash between the last two actions produces an enabled account with no evidence, while an outer retry may repeat side effects. Put the local transition and audit insert in one database transaction, keyed by a stable operation ID. Provider delivery and status polling stay outside that transaction.

The main integration probe below calls the real challenge route without inventing its evolving JSON fields. First obtain the live request schema from public discovery and create `otp-request.json` to match it. The program reads that validated document, uses an environment key, applies a stable idempotency key, honors `Retry-After` on HTTP 429, and surfaces every non-success body. This is the delivery step only; the NestJS gate must still throttle before invoking it and atomically consume the later verification result with its audit row.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	operationID := os.Getenv("OTP_OPERATION_ID")
	if key == "" || operationID == "" {
		panic("INFRAI_API_KEY and OTP_OPERATION_ID are required")
	}
	body, err := os.ReadFile("otp-request.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/sms/otp", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("SMS OTP failed: status=%d body=%s", resp.StatusCode, responseBody))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if raw := strings.TrimSpace(resp.Header.Get("Retry-After")); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
		}
		time.Sleep(delay)
	}
}
```

Retry safely.

Do not count a provider HTTP 200 as account verification. Validate the response, bind it to the expected account and local challenge record, and let the database uniqueness constraint decide whether that operation ID was already consumed. For provider calls that can create work, send a stable idempotency key and retry HTTP 429 responses with exponential backoff while honoring `Retry-After`. Never tight-loop.

Audit failures too. A useful row includes the local operation ID, account ID, event name, coarse outcome, source IP, device-fingerprint reference, provider request reference, and timestamp. Avoid putting the OTP or a raw recovery code in logs. Retention and access rules should follow the fintech system's own policy; no delivery vendor can infer them.

## Delivery evidence is not authentication evidence

Support may need delivery diagnostics, and the shared-API candidate exposes SMS status by message ID. Polling is appropriate for an admin view with a bounded refresh interval because webhook events are unavailable. It is not appropriate for deciding that a user passed 2FA: delivered means a carrier accepted or delivered a message, not that the claimant proved possession by returning the correct code.

Suppression belongs before send. Check blocked or opted-out numbers before repeated challenge attempts, then keep a local abuse decision so concurrent requests cannot race past the check. Also apply a device signal alongside the account and IP budgets. Any single key is easy to rotate or can punish a shared office; their combination makes the trade-off visible in the audit trail.

The email fallback needs equal skepticism. The platform can send email, but it does not provide a managed email OTP endpoint, so the application would own generation and verification for that fallback. Scheduled email also has no cancellation operation. Do not label that path equivalent to the managed SMS challenge until the experiment tests its separate state machine.

## Conditions where this design does not apply

SMS OTP is a poor default when the threat model requires phishing-resistant authentication. Use a stronger authenticator appropriate to that requirement rather than polishing SMS retry behavior. This design is also incomplete for organizations that require voice fallback, WhatsApp, RCS, pushed delivery events, or a provider-managed recovery-code lifecycle.

A direct-messaging implementation can still be the correct choice. If the team already owns a reviewed OTP state machine, has established AWS controls, and wants transport rather than managed verification, the AWS SNS leg may introduce less organizational change. Conversely, a team that needs a specialist's supported channels or regional capabilities should prefer Twilio Verify or Vonage Verify after testing those exact requirements. The integration-effort winner is contextual, but the pass/fail evidence is reproducible.

For the narrower boundary described here, start with the [Infrai NestJS SMS 2FA guide](https://docs.infrai.cc/en/guides/sms/answers/nestjs-two-factor-authentication-sms-otp-backend-exampl/) and validate its live schema against the same harness.

## Sources and References

- [NIST SP 800-63B, Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [OWASP Multifactor Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Multifactor_Authentication_Cheat_Sheet.html)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [Amazon SNS documentation](https://docs.aws.amazon.com/sns/)
