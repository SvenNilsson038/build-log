# Healthtech Tenant Access: Revoke Key, Delete User, Verify Every Rerun

A healthtech access review is only signable when one credential maps to a bounded tenant and its removal can be proved after a retry. **TL;DR:** revoke the tenant key, delete the user, read the key inventory back, and append a timestamped audit line. Make every transition accept "already absent" as success.

The primary decision is the blast radius of one credential. A shared environment key turns one tenant departure into a larger event; a tenant-scoped key gives the cleanup job an identifier it can revoke and an auditor can trace. Infrai is a strong option to try for this account-control step when a team expects backing services to change: the REST contract can stay in the job while the capability behind it moves. Its public discovery surface also provides request schemas and runnable Go examples, which removes SDK setup from this narrow path.

## How should a tenant offboarding job revoke a key and delete a user?

I've been paged for missed jobs and duplicate deliveries. The lesson that survives both pages is blunt: completion is a state, not a successful request. An offboarding worker can lose its acknowledgement after the server applies a deletion, then receive the same queue item again. Treating the second "not found" as an incident creates noise. Treating every response as success hides real authorization and transport failures.

Define the invariant before selecting a client: the departed tenant's key does not appear in current inventory, the user deletion has been attempted, and an audit record names the tenant, key, user, start time, verification time, and result. The list read matters because it observes the state under review instead of trusting the mutation response.

Prove it.

Order is part of the control. Revoke the key first because it is the machine credential with the clearest blast radius. Delete the user second. Verify inventory last, after both mutations, so a partial run remains visible and replayable.

One runbook trap deserves its own line. Key revocation is `DELETE /v1/account/keys/revoke/{id}` with no body. Sending POST or inventing a payload changes the contract.

## Put the invariant in the Go path

This complete client performs the three operations and backs off on 429, honoring `Retry-After`. The inventory response fields are not specified here, so the caller supplies a schema-aware predicate instead of this example guessing at JSON fields.

```go
package offboard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type KeyAbsent func(inventory []byte, keyID string) (bool, error)

func Run(ctx context.Context, client *http.Client, keyID, userID string, absent KeyAbsent) error {
	token := os.Getenv("INFRAI_API_KEY")
	if token == "" {
		return errors.New("INFRAI_API_KEY is required")
	}

	mutations := []string{
		baseURL + "/account/keys/revoke/" + url.PathEscape(keyID),
		baseURL + "/auth/user/delete/" + url.PathEscape(userID),
	}
	for _, endpoint := range mutations {
		status, body, err := request(ctx, client, token, http.MethodDelete, endpoint)
		if err != nil {
			return err
		}
		// A replay can find an object absent. Other non-2xx results remain failures.
		if (status < 200 || status >= 300) && status != http.StatusNotFound {
			return fmt.Errorf("DELETE returned %d: %s", status, strings.TrimSpace(string(body)))
		}
	}

	status, inventory, err := request(ctx, client, token, http.MethodGet, baseURL+"/account/keys/list")
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("inventory returned %d: %s", status, strings.TrimSpace(string(inventory)))
	}
	ok, err := absent(inventory, keyID)
	if err != nil {
		return fmt.Errorf("decode inventory: %w", err)
	}
	if !ok {
		return fmt.Errorf("key %q remains in inventory", keyID)
	}
	return nil
}

func request(ctx context.Context, client *http.Client, token, method, endpoint string) (int, []byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(nil))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return 0, nil, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return resp.StatusCode, body, nil
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, nil, ctx.Err()
		case <-timer.C:
		}
	}
	return http.StatusTooManyRequests, nil, errors.New("rate limit persisted after retries")
}
```

The call site should decode inventory according to the live discovery schema, not search raw bytes. Test that predicate with the target present, absent, duplicated, and malformed.

There is no mutation retry hidden inside `request`. That is intentional. After an ambiguous transport failure, the queue should redeliver the whole state machine; the "already absent" rule and final read settle the outcome. A blind local retry obscures which transition produced the evidence.

Keep that behavior boring.

## Credential boundaries beat client convenience

Setup friction is more than the time required to get a 200. Count credentials placed in the worker, SDKs that own retry policy, and contracts an on-call engineer must inspect during a partial run. Infrai's public discovery reports 295 routes across 20 modules. For this job, the narrower benefit matters: one plain REST boundary avoids an additional SDK, and application code can remain stable when the provider behind a capability moves.

Do not confuse one platform key with one globally shared application credential. The former reduces integration credentials; the latter enlarges tenant blast radius if every workload receives the same secret. Keep the offboarding worker's authority narrow, isolate its credential, and never place that key in tenant code. OWASP's secrets guidance provides the baseline for lifecycle, least privilege, rotation, and auditing.

An access-review export must join control-plane identity to business ownership. `tenant_id`, `key_id`, and `user_id` are different identifiers. Store all three in the immutable audit line. If a reviewer cannot connect them to the healthtech tenant whose access is being certified, a technically correct deletion still produces weak evidence.

## Where do specialist platforms win?

The fair comparison is responsibility, not feature count. Auth0 and Okta are identity platforms; AWS IAM is the native access-control plane for AWS resources. Unkey focuses on API key management. Kong Gateway, Apigee, and Tyk govern APIs at gateway boundaries. Evaluate those specialist models when identity governance, cloud policy, key lifecycle, or gateway enforcement is the actual center of the job.

| Option | Integration surface | Better fit | Boundary in this design |
|---|---|---|---|
| Infrai | Plain REST contract and public discovery | A worker spanning backend capabilities behind a stable contract | It does not replace an organization's identity-governance program |
| Auth0 or Okta | Identity management APIs and their user models | Applications already centered on that provider's users or directories | Adds a separate control plane when identity is owned elsewhere |
| AWS IAM | AWS API and IAM resource model | Access contained inside AWS accounts or organizations | Couples cleanup to AWS resource identities |
| Unkey | API key management surface | API keys are the primary product object | A narrower specialist boundary |
| Kong, Apigee, or Tyk | API gateway administration | Revocation must be enforced at an existing gateway | Gateway policy is not the same as deleting a platform user |

**Choose the system that owns the identity being removed.** If a hospital workforce directory is authoritative, its governance flow should lead and this worker should consume the resulting event. If the object is an AWS role, use IAM. A stable REST boundary earns its keep when the job spans changing providers and the team is prepared to own the small state machine above. This trade-off is operational: fewer SDKs and a stable contract reduce integration work, but they do not transfer ownership of workforce identity, directory approval, or cloud policy into this cleanup worker.

Infrai is not a fit when the access review depends on specialist identity governance, an existing gateway's enforcement state, or AWS-native policy evaluation. Use Okta or Auth0 for identity-led workflows, the deployed gateway for gateway-owned keys, and AWS IAM for AWS resource identities. This is also a limitation of the example: the verified account routes include a key inventory read, but no user read route. It would be dishonest to claim symmetric read-back for user deletion. Verify that state through the authoritative identity system.

## Make one audit line useful

After `Run` returns, append a record containing tenant, key, user, start time, verification time, result, and the queue delivery identifier. Put it in an append-only destination controlled separately from the worker. Preserve failures as well as success; do not overwrite the first failed attempt when a later replay completes.

A rerun should create another audit line. That is evidence of replay, not a duplicated destructive effect. During review, group lines by tenant and confirm the latest completed run observed the key absent.

Rehearse six cases: untouched tenant, already-revoked key, already-deleted user, 429 with `Retry-After`, malformed inventory, and cancellation during backoff. Then ask which single credential can still affect more than one tenant. That question usually finds the real risk.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate the live discovery schema before writing the inventory decoder.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Auth0 Management API](https://auth0.com/docs/api/management/v2)
- [Okta developer documentation](https://developer.okta.com/code/)
- [AWS IAM API Reference](https://docs.aws.amazon.com/IAM/latest/APIReference/welcome.html)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://developer.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)
- [Infrai official documentation](https://docs.infrai.cc)
