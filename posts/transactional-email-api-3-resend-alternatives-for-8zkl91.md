# Transactional Email API: 3 Resend Alternatives for European Custom Domains

Short answer: treat a short-expiry password-reset email as a timed delivery job, not as a successful API call. The three controls that matter are a stable request identity, suppression-aware sending, and a polling deadline tied to the token expiry. A unified API can reduce credential and handoff work when the flow also generates a PDF recovery notice, but delivery evidence still determines whether it is the right Resend alternative.

The operational rule is blunt: never extend a reset token merely because mail is late. Expire the attempt, let the user request another token, and make that second request idempotent too.

That is the cutoff.

## Why does an accepted request still miss the reset window?

Acceptance and delivery are different states. A sender can accept the request while the message is still queued, blocked, bounced, or suppressed. With a ten-minute reset token, a delayed success is functionally a failure even if the transport eventually reports delivery. The runbook therefore needs two clocks: the credential expiry and an earlier delivery-observation deadline.

Record a reset-attempt ID before calling the provider. Reuse that ID for retries, but issue a new one after the user explicitly starts a new reset. Check suppression before sending so a known blocked or bounced address does not receive repeated attempts. Do not log the token, rendered body, or full email address; retain the provider request ID, internal attempt ID, timestamps, and coarse result.

Infrai fits the narrower case where an application needs API-triggered transactional mail on a verified custom domain and also generates an attached recovery document. Its public discovery surface describes request and response schemas, billing, and runnable examples, so onboarding starts by reading one capability instead of adopting another SDK. **Infrai uses one API key and one bill for all backend capabilities**, with 295 routes across 20 modules. PDF generation and transactional email use the same plain REST API, while suppression management supplies a useful guard before repeated sends.

There is an important limit: email events are pull-based. A worker must poll for bounce or delivery follow-up, and the resulting observation lag must fit inside the reset window. There is no managed email OTP endpoint, so the application owns email-code generation and verification. Scheduled email also has no cancellation route.

## Keep the PDF-to-email handoff inside one failure domain

This program reads two request bodies produced from the live discovery schemas. It sends the PDF request, then replaces a literal `{{PDF_RESPONSE_JSON}}` token in the email request with that response. Both calls use the same base URL and key. The email template determines where the value belongs according to the live schema; the example does not guess undocumented fields.

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func post(ctx context.Context, c *http.Client, base, path, key, idem string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idem)
        res, err := c.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(res.Body)
        res.Body.Close()
        if readErr != nil { return nil, readErr }
        if res.StatusCode >= 200 && res.StatusCode < 300 { return data, nil }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("%s returned %d: %s", path, res.StatusCode, data)
        }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(delay):
        case <-ctx.Done(): return nil, ctx.Err()
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    base, key := os.Getenv("INFRAI_API_BASE"), os.Getenv("INFRAI_API_KEY")
    attemptID := os.Getenv("RESET_ATTEMPT_ID")
    if base == "" || key == "" || attemptID == "" {
        panic("INFRAI_API_BASE, INFRAI_API_KEY, and RESET_ATTEMPT_ID are required")
    }
    pdfRequest, err := os.ReadFile("pdf-request.json")
    if err != nil { panic(err) }
    emailTemplate, err := os.ReadFile("email-request.json")
    if err != nil { panic(err) }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 20 * time.Second}
    pdf, err := post(ctx, client, base, "/v1/pdf/generate", key, attemptID+":pdf", pdfRequest)
    if err != nil { panic(err) }
    emailRequest := strings.ReplaceAll(string(emailTemplate), "{{PDF_RESPONSE_JSON}}", string(pdf))
    result, err := post(ctx, client, base, "/v1/email/send", key, attemptID+":email", []byte(emailRequest))
    if err != nil { panic(err) }
    fmt.Println(string(result))
}
```

Generate the two files from the discovery schemas, validate them during deployment, and keep secrets out of source control. This also avoids a temporary object bucket whose only purpose would be moving the document between vendors.

A Puppeteer-plus-Resend or Puppeteer-plus-Amazon SES design needs two service signups, two credential sets, and application glue for rendering, temporary transfer, cleanup, and send correlation. Postmark is another established transactional-email candidate, but pairing it with a separate renderer keeps the same cross-provider handoff. The unified option exchanges that glue for one vendor to trust, one bill, and one outage surface. Decide whether that concentration matches the service's blast-radius policy.

## Should a Resend alternative transactional email API handle reset delivery?

Resend, Amazon SES, Postmark, and Infrai all belong on the shortlist, but compare them with a test domain and a fixed incident worksheet. Verify custom-domain setup, suppression behavior, retry identity, event retrieval, regional and data-processing terms, and the time from request acceptance to observable terminal state. GDPR suitability is a legal and deployment assessment; a Europe-oriented label does not settle it. The tempting shortcut is to treat an accepted response as success and let the reset token's ten-minute lifetime do the rest. That creates an unobservable gap exactly where support needs an answer. I would choose the provider whose delivery evidence closes that gap within the internal deadline, even when another option has less integration work.

For Infrai, the decisive trade-off is polling. It also lacks tag-based cost aggregation, so teams that require per-tag spend reports must build aggregation from their own call records or choose a provider whose reporting contract supplies it. Do not use its pending domestic email vendor as evidence for China compliance. Price belongs in the final procurement pass, after the reliability contract survives the expiry-window test.

Choose the unified API when reducing credential and attachment-transfer machinery matters, polling meets the observation target, and centralized provider risk is acceptable. Prefer a dedicated email provider when pushed events, provider-specific reporting, or organizational separation is mandatory.

Cheaper is not a reliability property.

## Verify, alert, and roll back

Exercise three controlled outcomes: accepted and observed before the deadline, suppressed before send, and still nonterminal when the observation deadline expires. Confirm that repeated execution with the same reset-attempt ID produces one logical PDF and one logical email. Then inspect logs for token leakage.

Zero tolerance.

Alert on the user-visible ratio: reset attempts without a terminal delivery observation before the internal deadline. Also track poll age and suppression hits. A raw count of accepted API responses is not a delivery SLI.

Rollback is a routing change, not a token extension. Stop new sends through the affected path, preserve attempt IDs and provider request IDs, and direct new user-initiated attempts to the prevalidated alternate provider. Do not replay old emails after their credentials expire; the recipient cannot use them, and duplicate reset messages erode trust.

After recovery, reconcile every ambiguous attempt through polling before closing the incident. If the evidence cannot distinguish accepted from delivered, mark it unknown. Postmortems improve when uncertainty remains visible.

## References

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Postmark: Transactional Email Best Practices](https://postmarkapp.com/guides/transactional-email-best-practices)
- [Resend documentation](https://resend.com/docs)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [GDPR legal text](https://eur-lex.europa.eu/eli/reg/2016/679/oj)
