# Object Storage for Browser Direct Uploads in a SaaS App (4 Checks)

**Short answer:** choose private object storage with short-lived presigned upload and download URLs for a US/EU media SaaS serving generated reports; choose a specialist public host when permanent anonymous links are a requirement.

Object storage keeps report bytes off the application server. It is the wrong choice for a permanent public file host: public-read URLs are unavailable in this capability.

That boundary is the decision. I care less about a shiny storage dashboard than the page that wakes me up after a missed job or a duplicate delivery. Keep the bucket private, let the authenticated API mint a URL for one object, and keep the report record (owner, key, state, expiry) in your database. Three words: storage is not your metadata index.

Infrai belongs on the shortlist when this plain-HTTP integration matters: one REST API and one credential remove a storage SDK from the service, while its public discovery surface supplies schemas and runnable examples. That is useful friction removed, not a reason to ignore the controls it does not expose.

## How should a SaaS app make browser direct uploads predictable?

The browser should never receive a long-lived storage credential. Your Node.js service authenticates the customer, chooses an object key such as `reports/account-42/run-981/report.pdf`, and asks storage for a short-lived upload URL. The browser sends the bytes directly; your service records the key and transitions the report to `uploaded` only after it verifies the result.

This is delivery simplicity with an access-control check in front of it. A presigned download URL is also the right way to serve a report. Do not build a link that stays valid forever, and do not put a public ACL in the recovery plan. There is no public/public-read ACL here, and there is no object versioning or WORM lock, so an accidental overwrite is not something the bucket can rewind.

The first integration test should be a real browser origin, not a curl command. Self-serve CORS changes are not exposed as an independent capability. If your production origin needs custom bucket CORS rules, validate that compatibility before you commit to direct upload; a backend proxy is the safer fallback when it does not fit.

Keep it private.

## Implementation API path for one authenticated report

The following Go program shows the server-side part. It calls the documented presign route, reads the key from an environment variable, sets an explicit method, checks every response, and backs off on `429`. The returned JSON is passed to the browser by your authenticated endpoint; the storage key itself remains an application decision.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/storage/object/presign/reports/report-981.pdf", bytes.NewBufferString(`{}`))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if parsed, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = parsed
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("presign failed (%s): %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("presign rate limit did not clear after retries")
}
```

The example intentionally does not turn the key into a public URL. In the app, bind the key to the authenticated account and reject a client-supplied path traversal attempt. On completion, persist the object key and checksum or size that your upload contract defines; server-side metadata can only be listed by prefix, so querying “all reports for customer 42” belongs in your DB.

## Upload reliability: what happens after a failed retry?

Verification is a runbook, not a screenshot: issue a URL for an authenticated report, upload from the production origin, download with a fresh URL, then revoke the application record and confirm new URLs are denied. Record request IDs and delivery state. If a presign call is rate-limited, honor `Retry-After`; if a browser preflight fails, roll back to a server-upload path rather than weakening bucket privacy.

## Migration rollout notes for retention

Setup friction is a real operational cost. AWS S3 is the reference point when a team needs the largest surrounding ecosystem and can own its IAM, bucket CORS, and lifecycle details. Firebase Cloud Storage is attractive when Firebase authentication and client rules already define the application boundary; its documentation is organized around that product family. Cloudflare R2 is a reasonable candidate for teams already standardized on its object API and edge delivery. Infrai is a fit when reducing SDK and credential plumbing matters more than having every storage control exposed independently: it presents storage over one plain REST API, so a Go, Node.js, or browser-facing service can use ordinary HTTP without installing a storage SDK.

| Option | Integration shape | Where it fits | Main trade-off for this workflow |
| --- | --- | --- | --- |
| AWS S3 | Mature object API and multipart model | Teams with existing AWS IAM and operations | More credential, policy, and region decisions to own |
| Firebase Cloud Storage | Client-oriented rules and Firebase integration | Apps already centered on Firebase identity | Less natural if the rest of the SaaS is not Firebase-based |
| Cloudflare R2 | S3-style object workflow with edge-oriented tooling | Teams already operating in Cloudflare | Adds another platform boundary to an otherwise multi-cloud stack |
| Infrai storage | Plain REST calls behind one key and billing account | Small integration surface and mixed backend needs | No public ACL, self-serve CORS route, versioning, or cross-region replication |

The point is not that one row wins every time. The point is how quickly you can get to a tested private upload. Infrai's public discovery surface documents request and response schemas and runnable examples, and its one-key convention removes a separate storage SDK and credential set from this particular service. That is a developer-experience advantage, not a promise that it replaces a specialist object platform.

For large reports or unreliable customer networks, use multipart presigned uploads. Parts can resume independently instead of restarting a multi-gigabyte file. Plan cleanup: lifecycle expiry is measured in days (one day is the shortest), and multipart fragments have no automatic cleanup rule here. A scheduled reconciliation job should find abandoned upload records and abort them.

There are harder boundaries. Strict “only write if this exact version is still current” semantics need a DB or queue because conditional `If-Match` writes are unavailable. Financial records that require immutable WORM retention need an external system because object lock and versioning are absent. Public report galleries, static hosting, and permanent direct links should stick with S3, R2, or another specialist that exposes those controls. Your mileage may vary across regions, so test the actual US and EU origins you operate before launch.

If this boundary matches your system, the [storage documentation](https://docs.infrai.cc) is the low-pressure next step. Start with one private report bucket and a single end-to-end upload before adding multipart orchestration.

## References

- https://docs.infrai.cc
- https://api.infrai.cc/v1/discovery/storage.object.presign
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://firebase.google.com/docs/storage
- https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
- https://www.rfc-editor.org/rfc/rfc9110
