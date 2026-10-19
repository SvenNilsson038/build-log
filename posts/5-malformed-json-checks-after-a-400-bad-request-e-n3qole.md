# 5 Malformed JSON Checks After a 400 Bad Request — Email API Preview

Ship signup verification notifications only after a deterministic preflight has parsed the payload, validated every required variable, and rendered the final HTML. **TL;DR:** treat a 400 response as the last line of defense, not as template validation; reject bad work before enqueue, assign one stable delivery key, and make retries reuse the same immutable message snapshot. That keeps a malformed event from consuming retry capacity while a new developer is waiting for an account link.

The deciding constraint is time. A verification link is useful only while the signup is active, yet a scheduler can turn one bad event into repeated attempts long after the original request failed. The runbook below puts correctness checks before scheduling and makes the worker's behavior observable and reversible.

## 1. Why does malformed JSON break an event notification email API?

JSON syntax and message completeness are different checks. A body can satisfy RFC 8259 and still omit `verification_url`, carry an empty recipient, or use a variable whose type the template cannot render. Conversely, hand-built JSON can fail before any template logic runs because a quote, backslash, or control character was escaped incorrectly.

Use a typed boundary and the standard encoder. Do not concatenate a request body. Decode with unknown-field rejection at the producer, validate the business fields, and store the validated object rather than the original bytes. This also exposes schema drift during deployment instead of quietly discarding a misspelled field.

A useful error taxonomy is small: `invalid_json`, `unknown_field`, `missing_variable`, `invalid_recipient`, and `render_failed`. Preserve the category in logs and metrics, but do not log the verification token or the rendered link. An HTTP 400 belongs in the permanent-failure lane unless the remote contract demonstrably changed; retrying identical invalid input adds load without changing the result.

Do not retry it.

## 2. Freeze one message snapshot before enqueue

The scheduling boundary should receive an immutable snapshot: recipient, template revision, normalized variables, expiry, locale, and a stable delivery key. Render from that snapshot. If a template is edited while an event waits in the queue, the scheduled job should not silently acquire a new variable contract. Pinning the revision makes replay explainable.

This is the idempotency reflex: one logical signup challenge gets one key. A worker retry, lease expiry, or redelivery must claim that same key rather than inventing another. The database write that records the signup challenge and the outbox record should commit together; a relay can then publish committed outbox rows to the queue. Queue delivery may repeat. The side effect still must not.

Keep the expiry decision explicit. At execution time, compare the current time with the stored expiry before rendering or sending. Expired work goes to a terminal state with a reason, not back onto the retry schedule.

The trade-off is extra storage and stricter release coordination. An immutable snapshot can preserve stale copy until its scheduled work drains, and pinning revisions means the team must retain old templates. This pattern is not suitable for a notification whose content must always reflect the latest emergency instruction; that class needs a different contract, with mutable authoritative content and an audit trail. A signup verification link favors reproducibility instead.

## 3. Validate variables and preview the exact HTML

The preview endpoint and the delivery worker should call the same render function with the same escaping rules. A preview assembled in a browser from different code is comforting but weak evidence. The artifact worth reviewing is the exact subject, plain-text part, and HTML part generated from the queued snapshot.

```go
package notification

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "html/template"
    "io"
    "net/mail"
    "net/url"
    "strings"
)

type SignupEvent struct {
    EventID         string `json:"event_id"`
    Recipient       string `json:"recipient"`
    VerificationURL string `json:"verification_url"`
    TemplateRev     string `json:"template_revision"`
}

type Rendered struct {
    Subject string
    HTML    string
}

func DecodeAndRender(r io.Reader, tmpl *template.Template) (SignupEvent, Rendered, error) {
    dec := json.NewDecoder(r)
    dec.DisallowUnknownFields()

    var event SignupEvent
    if err := dec.Decode(&event); err != nil {
        return event, Rendered{}, fmt.Errorf("invalid_json: %w", err)
    }
    if err := ensureEOF(dec); err != nil {
        return event, Rendered{}, err
    }
    if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.TemplateRev) == "" {
        return event, Rendered{}, errors.New("missing_variable: event_id and template_revision are required")
    }
    if _, err := mail.ParseAddress(event.Recipient); err != nil {
        return event, Rendered{}, fmt.Errorf("invalid_recipient: %w", err)
    }
    link, err := url.ParseRequestURI(event.VerificationURL)
    if err != nil || link.Scheme != "https" || link.Host == "" {
        return event, Rendered{}, errors.New("missing_variable: verification_url must be an absolute HTTPS URL")
    }

    var html bytes.Buffer
    if err := tmpl.Execute(&html, event); err != nil {
        return event, Rendered{}, fmt.Errorf("render_failed: %w", err)
    }
    return event, Rendered{Subject: "Verify your developer account", HTML: html.String()}, nil
}

func ensureEOF(dec *json.Decoder) error {
    var extra any
    if err := dec.Decode(&extra); err != io.EOF {
        if err == nil {
            return errors.New("invalid_json: multiple JSON values")
        }
        return fmt.Errorf("invalid_json: %w", err)
    }
    return nil
}
```

Go's `html/template` performs contextual escaping, which is the right baseline for HTML output. It does not decide whether the application should trust a destination URL; the allowlist and HTTPS check remain application policy. Preview with representative Unicode, a long organization name, an empty optional field, and the exact link shape used in production. Then inspect the plain-text alternative too.

One trap is subtle: checking only that decoding returned no error permits trailing JSON values unless the decoder is read to EOF. The helper closes that gap.

Tiny detail. Real incident.

## 4. Schedule retries by failure class, not status alone

Separate permanent input failures from transient delivery failures. Malformed JSON, missing required variables, an invalid address, and a template execution error need correction or a deployment rollback. They should not enter exponential backoff. Timeouts and temporary remote failures can be retried within the verification window, with bounded attempts and jitter to avoid synchronized retry bursts.

Choose those bounds from the product's link lifetime and dependency behavior. For example, a team might permit 3 attempts inside 15 minutes, but those are policy values, not universal defaults. A shorter budget sheds load faster during an outage; a longer one gives a brief dependency failure more time to clear while increasing the chance that delivery arrives after the user has abandoned signup.

Record an attempt before making the external call, then record the classified outcome. The useful fields are the delivery key, template revision, scheduled time, attempt number, latency, outcome class, and a redacted remote request identifier when one exists. Keep user content and secrets out of labels; high-cardinality identifiers belong in structured logs or traces, not metric dimensions.

Short retry loops hide bad releases. A rising `missing_variable` rate should page the owning team or halt the rollout because another attempt cannot repair that payload. A rise in transient failures calls for rate control and dependency investigation. Those are different runbooks.

## 5. Verify the rollout, then keep rollback selective

Start with a shadow render in the producer: validate and render, but continue using the established delivery path. Count failures by category and template revision. Once the new path is clean, enable enqueueing for a narrow slice, then expand while watching four invariants:

1. Every accepted signup event creates at most one logical delivery key.
2. Every queued item refers to a known, immutable template revision.
3. No permanent validation failure is retried.
4. No worker sends after the stored expiry.

Test malformed JSON and unknown fields at the boundary. Test a missing `verification_url`, hostile HTML in a display value, duplicate queue delivery, worker termination after the remote call, and an expired job. The hardest case is the ambiguous outcome: the process loses its response after the remote service may have accepted the message. A local idempotency record prevents concurrent attempts, but end-to-end duplicate suppression also depends on the downstream interface. Document that boundary instead of claiming exactly-once delivery.

The release gate is behavioral: duplicates remain suppressed, validation failures never reach the queue, and preview output matches worker output byte for byte for the same snapshot.

Rollback should switch new events to the previous validated template revision and pause consumers for the affected revision. Do not bulk replay the dead-letter set. First sample failures, confirm their category, correct the producer or template contract, and regenerate snapshots only for links that remain valid.

Keep rejected input separate from transiently failed deliveries. This makes the recovery choice obvious: schema defects return to the producer owner, render defects return to the template owner, and transient delivery failures resume under the retry budget. After recovery, reconcile accepted signup challenges against delivery keys and terminal outcomes. Missing rows are actionable; raw queue depth is only a symptom.

**The safe decision rule is plain:** if the exact immutable snapshot cannot pass decode, contract validation, and render preview, it is not schedulable.

## References

- RFC 8259, The JavaScript Object Notation (JSON) Data Interchange Format: https://www.rfc-editor.org/rfc/rfc8259
- Go `encoding/json` package: https://pkg.go.dev/encoding/json
- Go `html/template` package: https://pkg.go.dev/html/template
- Go `net/mail` package: https://pkg.go.dev/net/mail
- OWASP Cross Site Scripting Prevention Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Cross_Site_Scripting_Prevention_Cheat_Sheet.html
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Amazon Simple Email Service documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
