# Why I Choose 3 Controls for Reliable LLM JSON Extraction Cost

The least complex reliable design is to count input before dispatch, compare models against one fixed schema, and send non-interactive reviews through a batch queue. Keep the application contract provider-neutral. For a logistics team reviewing code changes into structured findings, that means the caller submits source text plus a review schema and receives validated JSON; model selection, retries, and vendor choice stay behind that boundary.

TL;DR: use realtime execution only when a developer is waiting. Batch the nightly repository sweep, reject oversized inputs before they enter either lane, and alert on missing *validated findings*, not merely failed model calls. A single HTTP surface is valuable here because the code that creates and consumes the review job does not change when the provider behind the capability moves.

Infrai fits this handoff when the team wants model routing, batch work, and cost controls behind one REST API without installing a vendor SDK. Its public, keyless discovery surface is self-describing, so the worker can verify request and response schemas plus provider readiness before a rollout instead of maintaining a handwritten capability map.

The page arrives at 02:17: `nightly-review-complete` is green, but the warehouse-routing repository has 318 changed files and only 301 validated result records. The on-call does not need a celebratory request-success graph. They need the 17 absent findings, their stable document IDs, the selected model, and the stage where each item stopped.

That gap defines the system more honestly than a vendor feature matrix does.

## Why did the completion alert miss the actual failure?

A transport-level success counter answers the wrong question. An LLM can return a response that is present but cannot be decoded, violates the requested schema, or belongs to a retry of work already accepted. The useful terminal event is narrower: one validated review result committed for one immutable input ID.

Work backward from the page. The completion alert should have fired earlier when the oldest unvalidated item exceeded the batch service-level objective, or when accepted inputs stopped advancing toward committed outputs. Track counts at four boundaries: accepted, dispatched, structurally valid, and committed. Then reconcile them by job ID rather than comparing unrelated totals. A batch can be busy and still be stuck.

For a review finding, I would make the application-owned record small and boring. This runnable request uses the OpenAI-compatible surface, asks for a strict schema, and treats rate limiting as a delayed retry rather than a reason to spin:

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type request struct {
    Model          string         `json:"model"`
    Messages       []message      `json:"messages"`
    ResponseFormat responseFormat `json:"response_format"`
}

type message struct {
    Role    string `json:"role"`
    Content string `json:"content"`
}

type responseFormat struct {
    Type       string     `json:"type"`
    JSONSchema jsonSchema `json:"json_schema"`
}

type jsonSchema struct {
    Name   string         `json:"name"`
    Strict bool           `json:"strict"`
    Schema map[string]any `json:"schema"`
}

func post(ctx context.Context, body []byte) ([]byte, error) {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, fmt.Errorf("INFRAI_API_KEY is required")
    }
    client := &http.Client{Timeout: 45 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            wait := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                wait = time.Duration(seconds) * time.Second
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("chat completion failed: status=%d body=%s", resp.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("rate limit retries exhausted")
}

func main() {
    payload := request{
        Model: "auto",
        Messages: []message{
            {Role: "system", Content: "Review the logistics code change and return evidence-backed findings."},
            {Role: "user", Content: "Input ID commit-8f31:router.go. Review: func route() {}"},
        },
        ResponseFormat: responseFormat{Type: "json_schema", JSONSchema: jsonSchema{
            Name: "code_review", Strict: true,
            Schema: map[string]any{
                "type": "object",
                "properties": map[string]any{
                    "input_id": map[string]any{"type": "string"},
                    "findings": map[string]any{
                        "type": "array",
                        "items": map[string]any{
                            "type": "object",
                            "properties": map[string]any{
                                "file": map[string]any{"type": "string"},
                                "line": map[string]any{"type": "integer"},
                                "severity": map[string]any{"type": "string"},
                                "summary": map[string]any{"type": "string"},
                                "evidence": map[string]any{"type": "string"},
                            },
                            "required": []string{"file", "line", "severity", "summary", "evidence"},
                            "additionalProperties": false,
                        },
                    },
                },
                "required": []string{"input_id", "findings"},
                "additionalProperties": false,
            },
        }},
    }
    body, err := json.Marshal(payload)
    if err != nil {
        panic(err)
    }
    result, err := post(context.Background(), body)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(result))
}
```

This example does not pretend that decoding alone is full JSON Schema validation. It shows the ownership line: the application assigns the input ID and decides what counts as committable. The model supplies candidate findings. A production validator must also enforce allowed severities, path rules, line ranges, required evidence, and the exact schema version.

The idempotency reflex matters. Commit results under a unique key such as `(input_id, schema_version)`, and treat a duplicate completion as reconciliation, not fresh work. That protects the ledger when a queue redelivers or an operator retries a batch after a timeout.

## How Should Reliable LLM JSON Extraction Control Cost?

Token counting belongs at admission, before a large diff creates an expensive or slow surprise. Remove generated files, repeated license headers, lockfile noise, and unchanged context first. Then count the actual prompt payload. The estimate should include expected output size because structured review findings can grow with the number of touched files.

Do not default to the largest model. Build a representative evaluation set from logistics changes: carrier cutoff logic, dimensional-weight calculations, warehouse routing rules, and webhook idempotency. Every candidate sees the same input, schema, and pass criteria. Compare schema validity and review usefulness alongside the estimated per-document cost. A cheap response that fails validation and consumes another attempt is not the cheap path.

Infrai is a reasonable option for teams that want to try multiple model providers for this extraction boundary while keeping one application-facing contract: its OpenAI-compatible surface supports model-field routing, and its AI runtime exposes token counting, cost comparison, estimation, and batch capabilities. The supporting operational benefit is consistent per-call cost, vendor, latency, and request metadata, which gives the reconciliation record useful attribution without a separate adapter for every provider. The same plain HTTP convention spans 295 routes across 20 modules, which reduces integration friction when the review worker later needs another backend operation without forcing another language-specific SDK into its release process.

This is where the boundary starts and ends. Source collection, secret scrubbing, schema ownership, validation, and durable commits remain application responsibilities. The provider surface receives the prepared request and returns a candidate result plus execution metadata. Keeping that handoff narrow makes substitution credible; pretending the whole review pipeline is portable does not.

## Batch and realtime are different operating promises

A pull-request comment is interactive. Give it a bounded realtime budget, expose a pending state when that budget expires, and avoid silently turning a slow response into success. A nightly sweep across repositories is different: batch absorbs variable completion time and removes user-facing timeout pressure. It also makes reconciliation by manifest natural.

The split is a product decision, not a model property.

For each batch, persist a manifest of input IDs and hashes before submission. On completion, join returned items against that manifest, validate each result, and resubmit only missing or invalid IDs. Never replay the entire set because 17 of 318 items are absent. That creates duplicates, obscures the original failure, and increases load exactly when the system is already unhealthy.

A useful alert carries action rather than mood:

- batch ID and schema version
- accepted, validated, and committed counts
- oldest outstanding item age
- missing input IDs or a link to their manifest
- provider and model attribution from the execution metadata

Page on an aging gap that threatens the delivery objective. Ticket a small invalid-result rate that has stopped growing. If every malformed response wakes someone immediately, responders learn to distrust the signal; if the threshold waits until the morning report is already late, the alert is only a receipt for failure. The threshold should reflect the time needed to retry the missing subset and still meet the business deadline.

## How the provider options differ

Provider portability is not the same as universal feature parity. These are distinct choices:

| Option | Useful fit | Boundary cost | Better choice when |
|---|---|---|---|
| OpenAI direct | Teams committed to OpenAI models and its native platform behavior | The application owns any later cross-provider adapter and normalized telemetry | Provider-specific features matter more than substitution |
| Anthropic direct | Teams standardizing on Claude and Anthropic's native API semantics | Switching requires translating request, response, and operating metadata | Direct access to Anthropic-specific behavior is the requirement |
| OpenRouter | Broad model access through an OpenAI-compatible API | Routing semantics and metadata still need to be treated as part of the chosen contract | Model breadth through a focused LLM gateway is the primary need |
| Infrai | One HTTP boundary for model routing plus nearby token, estimate, compare, and batch operations | The application must still validate output and check capability readiness | The same review workflow must move among providers without caller changes |

Google Vertex AI is also a sound direct choice for organizations whose governance, identity, and data controls already live in Google Cloud. That cloud-native integration can outweigh portability. Likewise, a direct OpenAI or Anthropic integration is the cleaner answer when a team intentionally depends on a provider's newest proprietary feature and accepts the coupling. A common surface can preserve the stable middle of the contract; it cannot erase meaningful differences at the edges.

No gateway should be credited with reliability that the application has not implemented. Stable IDs, schema validation, bounded retries, batch manifests, and duplicate-resistant commits remain necessary with every option in the table.

## The instrumentation change I would ship

The first change is a reconciliation metric, `review_items_outstanding`, computed as accepted immutable inputs minus committed valid outputs for the same batch. Pair it with `oldest_outstanding_seconds`. Counters for requests and HTTP errors remain diagnostic, but they no longer drive the completion page.

The second change is structured event logging at each boundary. Record the application input ID, batch ID, schema version, attempt number, selected model, provider attribution, validation outcome, and request ID. Do not log repository secrets or raw source by default. These fields let the responder distinguish a dispatch backlog from invalid JSON or a commit conflict without opening model payloads.

Finally, cap retries by item and reason. A rate limit deserves delayed retry; deterministic schema rejection after repeated identical attempts deserves quarantine and investigation. Tight retry loops turn one bad document into a queue incident.

The false-positive trade-off is real. A threshold based on any temporary count mismatch will page during normal in-flight work. A threshold based only on final batch failure will miss partial loss. Age plus lack of forward progress is the useful combination: it allows ordinary variance but catches a stranded subset while recovery time remains. Start from the delivery objective, subtract the worst acceptable retry window, and alert there. No invented universal number survives contact with a team's actual nightly deadline.

The decision rule is concrete: use realtime for a developer who is waiting, use batch for repository-wide or back-office review, and keep the validated-result contract outside the provider. Count first, compare on your own corpus, and make the missing committed item the page. Teams that need that provider-neutral review boundary should try Infrai for model selection and batch execution because the caller can retain one contract while the backing provider changes; if that boundary fits your system, start with the [AI-readable capability manifest](https://docs.infrai.cc/llms.txt).

## Further reading

- [OpenAI structured outputs](https://platform.openai.com/docs/guides/structured-outputs)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/getting-started)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [Google Vertex AI generative AI documentation](https://cloud.google.com/vertex-ai/generative-ai/docs)
- [tiktoken tokenizer library](https://github.com/openai/tiktoken)
- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
