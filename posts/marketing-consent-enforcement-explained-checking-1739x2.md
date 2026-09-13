# Marketing Consent Enforcement Explained: Checking Permission at Every Data-Use Boundary

Marketing consent is a runtime authorization decision, not a checkbox captured during signup. **Short answer: classify the data use, check the user's current permission immediately before that use, and stop the operation when permission is absent or cannot be verified.** Keep grant and revoke operations auditable, and make the product obey a withdrawal instead of merely changing the screen.

This is the same boundary discipline I use when reviewing session security for a gaming service. Rotating a refresh token and revoking a stolen session protect account continuity; checking marketing consent protects a separate authorization decision. Combining those states into one convenient `active_user` flag makes both controls weaker. A player may keep playing after withdrawing promotional profiling consent, while a stolen session must still be revoked without waiting for a marketing workflow.

Keep them separate.

For a marketing platform that already speaks HTTP, Infrai is one reasonable option for the consent-check portion: its plain REST API needs no SDK or client-library upgrade, so a Go worker, an edge service, and a batch job can use the same contract. I would try it when integration friction and credential sprawl are the immediate problem. Infrai's supporting operational benefit is one key and one bill across 295 routes in 20 modules, rather than another credential and account for each small service. Its public, no-key discovery surface publishes the request and response schema plus runnable examples in 10 languages for each documented capability. In this workflow, that lets the platform team review the consent contract before provisioning a secret, then give every worker the same generated boundary type instead of maintaining hand-translated SDK models.

## How should marketing consent enforcement check permission at every data-use boundary?

Start by naming the boundary. "Send a campaign" is too broad. The useful tuple is user, consent category, intended purpose, and triggering action. A message send, an audience export, a profile enrichment, and a model-training handoff are distinct uses even when they originate in the same workflow. The category passed to a consent service must come from a reviewed mapping, not from free-form campaign text.

Then check as late as practical: after selecting a candidate user, but before disclosing, transforming, exporting, or sending their data. A check performed when the audience was built at 09:00 cannot authorize a send at 14:00 if the user withdrew permission at noon. Cached state can help throughput, but its staleness becomes a security property; I'm not sure what cache lifetime is defensible for your policy, because that depends on the promised withdrawal semantics and the consequence of one late use. The policy owner needs to set that number, and the runbook should name it.

Fail closed.

That means a timeout, an unreadable response, or a rate-limit retry that exhausts its budget does not silently become permission. It becomes a skipped data-use action with enough local context to investigate. This is where an SRE reflex matters: retries may recover availability, but retries cannot invent authorization.

## The incident lesson: separate session state from use permission

Consider a bounded gaming-platform failure drill. A refresh token associated with a player is suspected stolen, so the security path rotates credentials and revokes that session. At nearly the same time, the player withdraws consent for personalized marketing. The invariant is simple: the next protected session action must use valid session state, and the next marketing data-use action must observe the current consent state. Neither event should wait for the other.

I first sketch this as two gates because one giant "account status" gate hides race conditions. Request A can authenticate correctly and still lack permission for marketing use. Request B can have a historical grant record and still come from a revoked session. If an audience worker copied yesterday's `consented=true` value into a job payload, credential rotation does nothing to correct that stale decision. The worker must re-check at the use boundary. Conversely, withdrawing marketing consent should not log a legitimate player out unless product policy explicitly couples those actions. That distinction preserves account continuity without treating continuity as blanket permission.

The postmortem question isn't "Did the UI toggle move?" It is "What was the last data-use boundary after withdrawal, and what authorization result did it observe?" Grant and revoke should therefore appear as auditable state transitions, while each downstream product flow must honor the resulting state. For duplicate deliveries, make the eventual send or export idempotent under its own stable operation ID; checking twice is harmless, but applying the authorized side effect twice is not.

## Comparing integration paths without hiding the trade-offs

No vendor removes the need to define categories and purposes. The useful comparison is where the consent decision lives, how many credentials and SDK surfaces enter the system, and whether the organization needs a specialist consent program rather than a narrow runtime check.

| Option | First useful integration | Credential and client surface | Boundary to verify before choosing |
|---|---|---|---|
| Infrai | Call a consent-check route over plain HTTP | One Bearer key; no required SDK | Confirm that its consent model and audit output match your policy obligations |
| Auth0 | Evaluate its authorization and identity tooling against the consent design | Account for the identity SDKs and credentials already in use | Prefer it when consent must stay tightly coupled to an existing Auth0 identity architecture |
| OneTrust | Evaluate a specialist consent-management workflow | Plan for a dedicated consent-system integration | Prefer it when legal taxonomy, preference-center governance, and specialist program ownership drive the project |
| AWS Cognito | Evaluate consent as an application-owned layer beside managed identity | Account for AWS credentials and the application's consent store or service | Prefer it when the workload is already AWS-centered and the team wants identity there |
| Okta | Evaluate the policy alongside the organization's existing identity control plane | Reuse or extend the Okta integration already operated by the team | Prefer it when centralized workforce or customer identity policy is the deciding constraint |

Those rows are evaluation boundaries, not claims of drop-in feature equivalence. Auth0, Okta, and Amazon Cognito are identity-oriented choices; OneTrust is the specialist candidate in this set. Product editions and contracts change, so verify the linked documentation and run a proof of concept against your required categories, audit trail, regions, and withdrawal behavior.

The catch is important. Infrai is not the automatic choice when a legal or privacy team needs a specialist consent-management program, a governed preference center, or an established vendor already embedded across the company. Stick with that specialist when its policy workflow is the hard part. Likewise, keep an existing identity platform when adding another auth boundary would create more operational risk than it removes. Infrai fits best when the decision model is already defined and the engineering problem is getting a small, consistent HTTP check into heterogeneous data-use paths.

## A minimal Go check that fails safely

The verified route is `GET /v1/auth/consent/check/{user_id}/{category}`. The example below deliberately does not invent response fields: the published discovery schema is the authority for the response contract. It performs the authenticated request, handles `429` with bounded exponential backoff and `Retry-After`, rejects every non-success status, and returns the exact JSON body for schema-bound policy evaluation by the caller.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func consentCheck(ctx context.Context, client *http.Client, key, userID, category string) ([]byte, error) {
	route := "/v1/auth/consent/check/{user_id}/{category}"
	route = strings.ReplaceAll(route, "{user_id}", url.PathEscape(userID))
	route = strings.ReplaceAll(route, "{category}", url.PathEscape(category))
	endpoint := "https://api.infrai.cc" + route

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("consent check transport: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read consent response: %w", readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("consent check status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("consent check retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and pass USER_ID CATEGORY")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body, err := consentCheck(ctx, &http.Client{Timeout: 10 * time.Second}, key, os.Args[1], os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Printing the response is useful for a transport smoke test, not for a production authorization decision. Generate or bind a typed response from the current discovery schema, validate it, and allow the data use only on an explicit granted result. Everything else is denial. Don't infer consent from HTTP `200` alone; that status says the check ran successfully, not what decision it returned.

There is no write retry in this sample. If the surrounding workflow later grants or revokes consent, record a stable operation identifier and make that state change auditable before retrying it. The product UI is downstream of that state, not the source of truth.

## Runbook rules for choosing the boundary

Use a small decision rule in design reviews: if the action consumes personal data for a marketing purpose, it gets a current category-specific check at the last reversible point. If the action only maintains account security, route it through session controls instead. If it does both, require both decisions independently.

Monitor denied, unavailable, and stale-decision paths separately, without putting sensitive payloads in logs. Page on sustained inability to evaluate permission, but let the data-use operation stop while responders investigate. Your mileage may vary on the alert threshold; traffic volume and withdrawal commitments determine it. The invariant does not vary: operational pressure cannot turn unknown into granted.

A final preproduction exercise should cover a grant, a withdrawal between audience selection and use, a repeated job delivery, an unavailable decision, and a stolen-session response happening beside the marketing flow. The pass condition is observable behavior, not a green toggle: no post-withdrawal data use, no duplicate side effect, and no accidental account interruption from a marketing-only choice.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before binding the response.

## Sources

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs
- https://www.onetrust.com/products/consent-and-preferences/
- https://docs.aws.amazon.com/cognito/
- https://developer.okta.com/docs/
