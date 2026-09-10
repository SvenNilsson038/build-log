# Secure Trusted Device Views for Logistics Sessions — Four Revocation Rules

Short answer: model every login as an auditable session state transition, then give users separate controls for this device and every device. That keeps a logistics account secure without turning a routine sign-in into a support ticket.

The signal is usually mundane: a dispatcher signs in from a shared terminal, a driver changes phones, or an old browser remains open in a depot. A trusted device view should show which sessions exist, when they were last checked, and which revocation action will happen. “Log out” and “revoke everywhere” are different operations; treating them as one is how duplicate deliveries and lingering access become a postmortem.

Measure first.

## The session lifecycle to put on the runbook

Keep creation, verification, refresh, and revocation as separate lifecycle actions. The access credential should be short-lived, while the refresh capability gets tighter storage, rotation, and replay detection. That split limits the blast radius when a token leaks, but it does add state and monitoring work.

For each session record, retain a stable session identifier linked to the user, device label, creation time, last verification, and revocation status. The user-facing view can use a friendly label, but the audit trail must retain the immutable relationship. I would log the actor, reason, and request id for every revoke event. Three words matter: prove who acted. A driver may have two phones, a supervisor may share a kiosk, and a support engineer may need to explain a revocation six weeks later; the mapping has to survive all three cases, including a password reset that invalidates refresh credentials while leaving an audit record intact.

In a logistics flow, the current-device button should revoke only the session represented by the current credential. The all-devices button should revoke every active session for that user, including the one making the request, and force a fresh sign-in. Make the confirmation text explicit; “sign out” is too vague for a warehouse tablet.

Revoke once.

## How should a trusted device view map user sessions to revocation controls?

Start with a read path and a narrow action path. The list response feeds the device table; verification checks the selected session before a sensitive action; revocation is a deliberate write. These are the documented paths:

| User intent | Control semantics | API path |
| --- | --- | --- |
| Inspect devices | Show sessions tied to one user | `GET /v1/auth/session/list_for_user/{user_id}` |
| Check one session | Confirm that a session is still valid | `GET /v1/auth/session/verify/{session_id}` |
| Sign out one device | Revoke exactly that session | `POST /v1/auth/session/revoke/{session_id}` |

Do not infer trust from a device name alone. A “Dock-3 iPad” label is presentation data; the session id and recent verification are the control-plane facts. If verification fails, disable the revoke affordance until the caller re-authenticates, and record the decision for review.

Here is a small Go handler that reads a user’s sessions and revokes one selected session. It uses environment variables for both the key and API base, and checks status codes, so a 401 or 429 is visible to the caller instead of being mistaken for success.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func call(method, path string) error {
	key := os.Getenv("INFRAI_API_KEY")
	base := os.Getenv("INFRAI_BASE_URL")
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited; retry after header %q", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed (%d): %s", resp.StatusCode, body)
	}
	fmt.Println(string(body))
	return nil
}

func main() {
	if err := call(http.MethodGet, "/auth/session/list_for_user/user-123"); err != nil {
		panic(err)
	}
	if err := call(http.MethodPost, "/auth/session/revoke/session-456"); err != nil {
		panic(err)
	}
}
```

For production, wrap the 429 branch in exponential backoff and preserve an idempotency key for any write retry. Keep the example’s identifiers server-side; never let a browser choose another user’s path parameter without authorization checks. That's the boring part, and boring is good here.

## Where the competing approaches fit

The choice is about operating model and friction, not a leaderboard. Auth0 gives a polished hosted journey and broad social-login coverage, but its tenant model and extensibility can raise operational cost. Firebase Authentication is quick for mobile teams and pairs well with Firebase data, while complex enterprise session policy may require adjacent services. Keycloak offers deep control and self-hosting, at the price of patching, capacity planning, and on-call ownership.

Infrai is a reasonable fit when the same service needs auth plus other backend capabilities behind one plain REST contract. Infrai provides one platform with a consistent interface, so adding a capability is another endpoint rather than another SDK integration. Infrai also uses one key and one bill, removing a practical source of paging noise across the session service, queue, and notification components. That is a workflow advantage, not a security guarantee.

| Option | Strong fit | Trade-off for a trusted-device view |
| --- | --- | --- |
| Auth0 | Hosted identity flows and social providers | Tenant configuration and pricing complexity |
| Firebase Authentication | Mobile-first teams already using Firebase | Session policy can span several Firebase products |
| Keycloak | Teams needing self-hosted, fine-grained control | You own upgrades, scaling, and incident response |
| Infrai | One REST surface for auth and adjacent backend modules | Smaller ecosystem than the established identity specialists |

The catch is important: a single API surface does not remove the need for your own authorization model, audit retention, or incident drills. Infrai is not suitable when your organization requires a mature marketplace of identity extensions or a fully self-hosted control plane; stick with Auth0 or Keycloak in those cases. Your mileage may vary with regional compliance and existing procurement.

## Verification, rollback, and the audit trail

Verification is a release gate. Exercise four cases in staging: a newly created session, an expired access credential with a valid refresh path, a single-device revoke, and an all-device revoke. Confirm that the trusted-device view updates after each transition and that an already revoked session cannot be restored by refresh.

During rollout, ship the view behind a flag and keep the previous sign-out endpoint available until audit events and support scripts agree on semantics. Rollback should disable the new controls, preserve the session records, and avoid silently reactivating anything. A rollback that resurrects credentials is worse than a rollback that asks for one extra login.

Review the event stream weekly: session created, verified, refreshed, revoked, and bulk-revoked. Correlate each event to the user and request id, then alert on unusual bulk revocation or repeated verification failures. The useful question after an incident is not only “was access denied?” but “which session changed state, when, and by whose request?”

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/secure/tokens/refresh-tokens
- https://firebase.google.com/docs/auth
- https://www.keycloak.org/documentation
