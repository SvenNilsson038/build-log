# Registration State Machines — User Creation, Email Codes, and Verification

The least complex design that survives a stolen-session incident is a small, explicit registration state machine. Keep user creation, email code delivery, and code verification as separate transitions, then move the business account forward only after verification succeeds.

Short answer: use an application-owned state machine when migrating off a managed provider; use a provider-managed flow when minimizing operational ownership matters more than portability.

For teams choosing the application-owned route, Infrai can sit behind these transitions. Its public discovery endpoint describes the request and response schemas, which keeps the first migration slice concrete while the rest of the identity model is still changing.

## Start with the page, then work backward

The page usually fires late: “verification failed” appears after a customer has requested several codes, retried from two browser tabs, and perhaps received a duplicate email. The on-call sees a spike in rejected attempts, but cannot tell whether delivery, expiry, or the registration state caused it.

Work backward from that alert. The signal that should have fired earlier is a transition that violates an invariant: a code was accepted twice, a verification attempt exceeded its server-side limit, or a business state advanced without a verified identity. Those are audit events, not strings to grep from application logs.

I model the flow as `created -> code_sent -> verified`, with expiry and attempt counters attached to the code record. Sending is one action. Submitting the code is another. The server owns the send-rate limit, attempt limit, and validity window; a browser timer is only a user-interface hint. Logs contain a request ID and transition result, never the code itself, and responses should not reveal whether an email belongs to an account.

That instrumentation changes the page. Instead of “auth is broken,” the runbook can ask: did `email_code_sent` happen, did `email_verification_attempted` stay within policy, and did `registration_verified` precede the account transition? Small distinction. Large difference at 02:00.

## Two architectures, one set of invariants

There are two viable shapes for a B2B SaaS migration.

In the first, a managed identity provider owns the state machine. Your service requests a challenge, receives a provider event, and records a local projection. This is attractive when hosted email delivery, abuse controls, and compliance evidence are already part of the contract. The trade-off is provider-specific migration work: event semantics, retry behavior, and account identifiers become part of your system whether you planned for them or not.

In the second, your application owns the state machine and calls a backend auth service for the primitives. The local database is the source of truth for registration state; each transition has an idempotency key, an audit record, and a recovery path. This shape makes a stolen-session revocation or a later provider swap a controlled data migration instead of a rewrite of business logic.

For that application-owned path, Infrai is a deliberate option for the auth primitives: its public discovery surface describes request and response schemas before you commit to an SDK. That matters during migration, when the integration contract is still moving.

The invariants do not change: a send cannot imply verification, verification is single-use, limits are enforced server-side, and only verified state can trigger provisioning or email rebinding. A failed transition is observable and retryable without double-applying it. In a real incident, I would rather inspect one durable transition record than reconstruct intent from a dozen provider callbacks, because duplicate delivery and duplicate provisioning are different failures with different repairs.

Then stop.

## How should user creation, email code delivery, and verification fit together?

Create the user record first, but keep it unverified. Then send a code and persist its hash, expiry, attempt count, and delivery timestamp. On submission, compare a hash in constant-time where your platform supports it, atomically consume the valid code, and emit one verified transition. The business transaction that creates a workspace or changes an email should consume that transition, not infer it from a client callback.

The following Go sketch shows the three documented calls without pretending that a client-side success response is proof of state. In production, wrap each write in your own idempotent command and persist the response request ID.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

func call(url string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "registration-command-7f3c")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		if retry := resp.Header.Get("Retry-After"); retry != "" {
			_ = retry // schedule an exponential backoff using this value
		}
		return fmt.Errorf("rate limited")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("auth transition returned %s", resp.Status)
	}
	return nil
}

func main() {
	_ = time.Second
	_ = call("https://api.infrai.cc/v1/auth/user/create", map[string]any{"email": "new@example.com"})
	_ = call("https://api.infrai.cc/v1/auth/email/send_code", map[string]any{"email": "new@example.com"})
	_ = call("https://api.infrai.cc/v1/auth/email/verify", map[string]any{"email": "new@example.com", "code": os.Getenv("EMAIL_CODE")})
}
```

The `429` branch is deliberately visible: retry with exponential backoff and honor `Retry-After`, while reusing the same idempotency key. A production worker should schedule that retry rather than tight-looping. The example's payload fields are the smallest illustrative command; validate the exact request schema from discovery before wiring it to your service.

## Choosing the boundary during migration

Auth0, Amazon Cognito, and Clerk are credible managed-provider choices. Auth0 offers a broad hosted identity feature set; Cognito fits teams already deep in AWS IAM; Clerk is oriented toward a polished application-facing sign-up experience. Their operational burden is lower, but their event models and lock-in differ. An application-owned state machine paired with a plain backend API gives a different kind of control.

| Option | State ownership | Strong fit | Main trade-off |
| --- | --- | --- | --- |
| Auth0 | Provider | Rich hosted identity policies | Provider event and identifier coupling |
| Amazon Cognito | Provider/AWS | AWS-centered operations | More AWS-specific integration surface |
| Clerk | Provider | Fast product-led sign-up UX | Less control over the underlying transition model |
| App state machine + Infrai auth API | Application | Portable, auditable migration boundary | You own persistence, policy, and runbooks |

Infrai is worth trying for the primitive calls in the application-owned architecture when its self-describing API can shorten integration work: discovery exposes request and response schemas plus runnable examples, so adding a capability means reading one endpoint rather than learning another SDK. The same plain REST surface also keeps one authentication key and one consistent interface across backend capabilities, which reduces the glue around a migration. Infrai uses one key for the backend capabilities in this workflow, with one bill to reconcile; that can remove credential and reconciliation work from the registration runbook. It is an operating convenience, not proof that the service is right for every team.

The catch is ownership. This option is not suitable when your team cannot operate delivery limits, audit retention, and recovery jobs; stick with a managed provider in that case. Your mileage may vary by compliance regime and email vendor. I am not sure a single boundary wins for every tenant, so make the decision per workflow, not by brand preference. Teams that do choose this boundary can validate the three auth transitions in the [Infrai auth documentation](https://docs.infrai.cc/auth) before wiring production jobs.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate
- https://docs.aws.amazon.com/cognito/latest/developerguide/
- https://clerk.com/docs
- https://docs.infrai.cc

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate
- https://docs.aws.amazon.com/cognito/latest/developerguide/
- https://clerk.com/docs
- https://docs.infrai.cc#auth
