# Zero-Downtime API Key Rotation — A 4-Phase Rolling Deployment Runbook

Short answer: for zero-downtime API key rotation, issue a second key, publish both versions through the secret store, make every new pod prove it can use the new key, and revoke the old key only after the Kubernetes rolling deploy and a measured grace period are complete.

For a property-management service that checks a prepaid account balance, the dangerous design is one credential shared by the API, cron monitor, queue worker, and an operator's laptop. Rotation then has the blast radius of the whole account. A safer rule is plain: one workload identity per failure boundary, two accepted key versions only during a planned transition, and one recorded decision that ends the overlap.

I've been paged by missed jobs and duplicate deliveries. That experience makes me distrust a rotation plan whose only proof is a green Deployment object. Green is useful; it is not evidence that every delayed job, old pod, and retrying process has stopped presenting the previous credential.

## Why the balance monitor needs a small credential blast radius

The balance monitor has a narrow job: read the prepaid balance on a schedule and emit an alert before the account runs dry. Its API key should have only the account and operation permissions needed for that job. Don't reuse the property-management API's key merely because both workloads call the same upstream account platform. If the monitor's key is disclosed, the response should not require rotating unrelated web and queue workloads at the same time. That separation also makes normal rotation tractable. The monitor can move through `old`, `overlap`, `new`, and `revoked` states without coordinating a release of every application in the estate. Store key material in the secret store, but keep lifecycle state outside the secret value: a rotation ID, the preferred version, the allowed fallback version, the start time, the grace deadline, and the approving workload identity. Logs should contain the rotation ID and a non-secret version identifier, never the key.

OWASP's Secrets Management Cheat Sheet treats creation, rotation, revocation, and expiration as parts of one secrets lifecycle. That framing matters here. Writing a new secret is not the finish line; proving adoption and revoking the superseded credential are part of the same operation.

One boundary deserves special treatment. Planned rotation may use overlapping credentials, while confirmed exposure should enter an incident path with immediate revocation as the security objective. A grace window is an availability tool, not permission to keep a known-exposed key active.

## How should zero-downtime API key rotation work across a Node.js Kubernetes rolling deploy?

Use four phases, and make the transition between them depend on evidence rather than wall-clock time alone.

1. **Issue.** Create a replacement credential with the same narrow account scope as the balance monitor. Record a rotation ID and publish the new version alongside the old version in the secret store. The old version is still preferred, so this phase changes no traffic.
2. **Adopt.** Flip the preferred version to the new key and start the Kubernetes rolling deploy. Each new Node.js process reads the versioned secret references during startup, uses the preferred key for its authenticated readiness check, and reports its release ID plus the non-secret key version that succeeded.
3. **Observe.** Keep both credentials accepted for a bounded number of grace hours. Require all desired replicas to report the intended release and successful new-key authentication. Then wait through a quiet period long enough to cover the monitor schedule, queued retries, secret propagation, and the audit pipeline's observed delay.
4. **Revoke.** Disable the old key, remove its reference from the secret store, and verify one positive request with the new key plus one negative request with the retired drill key. Preserve the evidence and approval under the rotation ID.

The Node.js application contract can stay small: load `primary` and optional `fallback` key versions at process start, prefer `primary`, and expose the successful version ID in a health signal. It should not try two credentials on every call. Fallback is allowed only while the lifecycle record says `overlap`, only after an authentication rejection such as `401`, and only once for an idempotent read. A `403` is an authorization decision, so retrying it with another key can conceal a scope error.

For the prepaid-balance monitor, the upstream request is a read, but the surrounding job still needs an idempotency reflex. Use the scheduled observation time as the job's stable identity. If a pod exits after fetching the balance but before acknowledging the queue item, the replacement may run the same check again; alert storage should deduplicate that identity rather than depend on the credential version. Key rotation and job delivery are separate state machines. Mixing them is how a harmless retry becomes a duplicate notification.

I'm not sure how many grace hours your system needs. Nobody can determine that from a deployment manifest. Measure the longest normal secret refresh, rolling-deploy duration, job interval, retry age, and audit-ingestion delay, then add an explicit operator response margin. For a drill, a two-hour window is a reasonable example input to test, not a universal recommendation. If normal evidence arrives after that deadline, stop automatic revocation and page the owner; don't silently extend an overlap that nobody approved.

## Make the overlap state explicit in the controller

Keep provider calls behind a narrow interface so the runbook doesn't depend on a vendor SDK. The controller below evaluates sanitized rollout evidence. It never reads key material, and it cannot authorize revocation merely because the deadline has arrived.

```go
package rotation

import (
	"fmt"
	"time"
)

type PodEvidence struct {
	Name          string
	Release       string
	Ready         bool
	NewKeyProven  bool
	LastOldKeyUse *time.Time
}

type Gate struct {
	ExpectedRelease string
	DesiredReplicas int
	QuietPeriod     time.Duration
	GraceDeadline   time.Time
}

func (g Gate) CanRevoke(now time.Time, pods []PodEvidence) (bool, []string) {
	var blockers []string
	if now.After(g.GraceDeadline) {
		blockers = append(blockers, "grace deadline passed; require operator review")
	}
	if len(pods) != g.DesiredReplicas {
		blockers = append(blockers, "replica evidence is incomplete")
	}

	seen := make(map[string]bool, len(pods))
	for _, pod := range pods {
		if seen[pod.Name] {
			blockers = append(blockers, fmt.Sprintf("%s: duplicate evidence", pod.Name))
		}
		seen[pod.Name] = true

		if pod.Release != g.ExpectedRelease {
			blockers = append(blockers, fmt.Sprintf("%s: unexpected release", pod.Name))
		}
		if !pod.Ready || !pod.NewKeyProven {
			blockers = append(blockers, fmt.Sprintf("%s: new key is not proven", pod.Name))
		}
		if pod.LastOldKeyUse != nil && now.Sub(*pod.LastOldKeyUse) < g.QuietPeriod {
			blockers = append(blockers, fmt.Sprintf("%s: old key used inside quiet period", pod.Name))
		}
	}

	return len(blockers) == 0, blockers
}
```

The intentionally boring return value is useful. The caller can write the blockers into the rotation record, alert on them, and refuse the destructive step. Test the gate with a missing replica, duplicate pod evidence, a stale release, an unsuccessful authentication probe, old-key use one minute inside the quiet period, and a passed grace deadline. Those cases are more valuable than a happy-path test because they define what must stop revocation.

Do not let the controller infer adoption from pod age. A fresh pod might still receive an old secret snapshot, and a ready pod might not have exercised its upstream permission. Require the positive authenticated check. Also reject duplicate evidence by workload identity; otherwise one chatty replica can make the set look complete while another replica is absent.

The monitor should emit enough context to reconstruct the decision: rotation ID, key version ID, workload identity, release, result class, and event time. It should not emit authorization headers, secret-store payloads, or a reversible fingerprint. Access to the evidence stream is itself privileged because it describes account structure and deployment timing.

Short-lived overlap is the trade-off. Two valid keys reduce deployment interruption, but they temporarily create two credentials that can access the account. This approach is not suitable when the upstream allows only one active key, the client cannot select a version deterministically, or usage records cannot distinguish the versions. In that case, use a scheduled maintenance window, or introduce a credential broker only after accepting that the broker becomes another availability and security boundary.

## Verify the rollout before touching the old key

Start the drill with synthetic property data and a dedicated drill credential. Record the expected replica count and release ID before changing the preferred secret version. Then watch two views at once: Kubernetes tells you which workload revision is serving, while the account-side authentication evidence tells you which credential version is actually being accepted. Neither view is sufficient alone.

The decisive check is a set comparison. Every desired pod identity must appear exactly once with the target release, readiness, and successful use of the new key. No old-key observation may fall inside the quiet period. The queue's oldest retry must be younger than the evidence boundary or explicitly drained. The scheduled balance check must complete under its stable job identity without producing a second alert. Only then does the operator approve revocation.

Keep it dull.

A useful pre-production drill deliberately pauses one old replica while the new replicas become ready. The gate should remain blocked. Resume the rollout, submit duplicate evidence from one pod, and confirm that the duplicate cannot stand in for the missing identity. Finally, advance through the full configured quiet period and verify that one late old-key event restarts it. These are controlled test inputs, not production incident anecdotes, and they expose timing assumptions before an urgent rotation does.

Observe the application after revocation as well. A positive probe using the new credential confirms continued access. A negative probe using the retired drill credential confirms that retirement took effect. Treat `401` as expected only for that negative probe; an authentication rejection from the active monitor should page the workload owner and attach the rotation ID. Avoid automated key resurrection. It erases the very security boundary that revocation was meant to establish.

## Roll back the deploy, not the credential decision

Before revocation, rollback is simple: stop the rolling deploy, return the old version to `primary`, and keep the replacement available while the team investigates. Record why adoption failed. Do not delete either secret reference while pods from both releases exist.

After the old credential is revoked, rollback means deploying the previous application release with the new credential. It does not mean re-enabling the retired key. This is why the application change that understands versioned references should be deployed and proven before the key transition begins. If the previous release cannot use the new reference contract, the team has coupled code rollback to credential rollback and should fix that dependency before scheduling the drill.

For a confirmed leak, the priorities change. Revoke the exposed key under the incident procedure, accept that the monitor may pause, and recover service with the replacement credential. A prepaid-balance alert arriving late is an operational problem; leaving an exposed account credential active can widen the incident. The runbook should state that choice before anyone is tired and looking at a clock.

Close the rotation only when its record can answer five questions: which workload owned the key, which release adopted the replacement, which evidence allowed revocation, who approved the decision, and whether the retired credential failed the negative probe. If the record cannot answer those questions without chat history, the next rotation will begin with guesswork.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
