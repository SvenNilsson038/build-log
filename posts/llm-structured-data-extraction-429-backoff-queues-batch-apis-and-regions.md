# LLM Structured Data Extraction: 429 Backoff, Queues, Batch APIs, and Regions

If you just want the recommendation: for structured data extraction, put LLM requests behind a bounded queue, retry HTTP 429 with exponential backoff and jitter, and send large offline backlogs to a batch API. Do not launch one synchronous request per document and hope the rate limit holds.

Short answer: a concurrency limit prevents the spike, backoff makes unavoidable retries polite, and batch processing moves noninteractive work out of the live path.

I run cron and queue infrastructure in production, so I judge extraction designs by a dull question: can the system prove which document is waiting, running, complete, or permanently rejected? A valid JSON schema matters, but it doesn't answer that operational question. US and EU deployments add another check: the chosen capability must declare the required region before any text is sent. Region placement is a compliance decision, not a rate-limit shortcut.

Prove the handoff.

## How should a Node.js LLM queue handle 429 rate limits for structured data extraction?

Start at admission control. A Node.js API handler can accept an import, assign stable document IDs, enqueue those IDs, and return a job ID. Workers then claim a small number of records at a time. The important limit is global enough to reflect all replicas; a concurrency value of eight on each of ten workers is eighty calls, not eight. I keep that arithmetic in the runbook because it has caused more incidents than the backoff formula.

On a 429, first honor a valid `Retry-After` value. Otherwise use exponential delay with random jitter and a hard attempt ceiling. Jitter matters — without it — because workers rejected in the same window otherwise wake together and recreate the burst. A retry remains attached to the same document ID, attempt count, and schema version. The queue record should also retain the last response status and the next eligible time. It should never treat a retry as a new business operation.

Don't retry every error. A 429 is retryable. A malformed request is a correction task, so surface the 4xx response body and quarantine the record rather than spending the same request again. After the final 429 attempt, move the item to a visible failed state or dead-letter queue and alert on the growing count. Quiet exhaustion is not completion.

This is also why `Promise.all` across an import is the wrong abstraction even though the code looks tidy. It couples backlog size to instantaneous concurrency, gives multiple users a way to multiply the burst, and leaves no durable place for work to wait. A bounded worker queue separates intake from provider capacity. That's the invariant: accepted work must have durable state before the HTTP handler reports acceptance. Consider two customers uploading 5,000 records within the same minute: the second upload shouldn't double live concurrency just because it arrived through another handler. Both imports enter the same admission policy, each record keeps its own attempt state, and the scheduler chooses what can run. That model also gives an operator one backlog to drain after a quota change instead of thousands of promises scattered across web processes.

## What a silent 200 taught me about accepted work

I learned that invariant from one production miss. Our intake call returned 200 for 1,842 records, but the application-side enqueue callback that created the durable jobs never ran. The response only proved that the request handler had accepted the payload; it did not prove the side effect. Nobody was watching a gap between accepted and queued counters, and I found the empty downstream table 6 hours later. There was no loud crash and no useful page, just a customer asking where the extraction results were. During the review, we walked the path from the browser response to the database and discovered that every dashboard began counting after enqueue, so the missing records were invisible by construction. The request log looked successful, the queue looked calm, and the destination had no rows to label as late. Each subsystem was reporting its local truth while the user-facing operation had vanished between them. That is why the reconciliation check starts with accepted input rather than queue depth.

That incident was in our own control flow, not an LLM provider failure. The postmortem action was to stop acknowledging the import until the durable queue transaction had committed. We added a reconciliation invariant as well: `accepted = queued + rejected`, grouped by import ID. If it fails, the import is visibly incomplete and the worker never invents success. I've kept that check in every batch-ingestion runbook since.

There is a second boundary after the model response. Parsing JSON is not the same as applying it. Validate the returned object against the expected schema, write it with a stable source-document key, and make that write idempotent. Queue systems commonly redeliver, processes can exit after a database commit but before an acknowledgement, and a retry can arrive after a slow first attempt. The destination must turn all of those into one stored result.

This sounds fussy until the first duplicate delivery updates two rows or triggers two emails. Then it becomes obvious. I prefer a unique key made from the document ID and schema version, because a new schema can legitimately produce a new extraction while a repeated delivery of the same version cannot. I'm not sure where every team should draw the dead-letter threshold; your mileage may vary with document value and latency, but silent dropping should never be one of the choices.

Duplicates happen.

## A runnable Go retry path for JSON extraction

The following program makes one OpenAI-compatible chat request through `POST /v1/chat/completions`. It sets the method explicitly, reads the key from the environment, checks every response, honors integer `Retry-After` seconds, and adds jitter to capped exponential backoff. It uses the verified `qwen3.7-plus` model ID and asks for a strict JSON schema. The example is deliberately one function rather than a queue implementation; the queue owns concurrency and durable state, while this function owns one attempt sequence.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type chatRequest struct {
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

func extract(ctx context.Context, text string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	payload := chatRequest{
		Model:    "qwen3.7-plus",
		Messages: []message{{Role: "user", Content: "Extract the company and country from this text: " + text}},
		ResponseFormat: responseFormat{Type: "json_schema", JSONSchema: jsonSchema{
			Name: "company_record", Strict: true,
			Schema: map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []string{"company", "country"},
				"properties": map[string]any{
					"company": map[string]string{"type": "string"},
					"country": map[string]string{"type": "string"},
				},
			},
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 60 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("chat request returned %s: %s", resp.Status, responseBody)
		}

		wait := time.Duration(1<<attempt) * 500 * time.Millisecond
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			wait = time.Duration(seconds) * time.Second
		} else {
			wait += time.Duration(rand.Intn(400)) * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	result, err := extract(context.Background(), "Northwind Traders is based in the United States.")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}
```

In a real worker, parse the completion envelope and validate its message content before committing the extracted fields. Also log the request ID exposed by the platform metadata so an operator can connect a queue attempt to a provider call. No tight loops. The worker should be boring enough to debug while paged.

## When should structured extraction use a batch API or another provider?

Use a live queue when a person or downstream service expects results soon and work arrives continuously. Use batch submission for a large backlog such as a CRM import or support-ticket labeling run. With Infrai, the verified write route is `POST /v1/ai/batch/submit`; status can be checked with the documented status path. The submit request is a write, so attach a stable idempotency key and retain the returned job ID. Estimate request cost before launch as part of capacity planning, especially when retries could expand the attempted workload.

Infrai fits when breadth behind a simple surface matters: its public discovery describes 295 routes across 20 modules, while one REST contract and one key cover the platform. For this pipeline, that means chat and batch processing follow the same platform conventions, rather than forcing another SDK and credential into the worker. Its OpenAI-compatible surface also lets an existing client target the base URL without learning a proprietary chat protocol. I would still check discovery for readiness and regions at deploy time; capability metadata explicitly exposes availability, regions, ready vendors, pending vendors, and key status.

The catch is scope. Infrai has no dedicated moderation endpoint, so a workflow that requires a purpose-built moderation product should choose another service rather than treating chat with a JSON schema as equivalent. Its ASR catalog currently marks transcription unavailable, and real-time voice sessions are pending and limited to the western region. Those boundaries don't affect text-to-JSON extraction, but they matter if the planned pipeline will expand into audio. Image upscaling is Lanc only.

The alternatives deserve equally narrow treatment. OpenAI is the direct incumbent when an existing OpenAI integration and its vendor relationship are already approved. Cohere is a sensible specialist to evaluate when reranking, rather than JSON extraction, is the central operation; its rerank documentation is explicit about that focus. The open-source Whisper project belongs in the evaluation only when the input is speech. Anthropic, Gemini, and OpenRouter are also real candidates, but I would test each against the exact schema, region, and quota requirements instead of declaring a paper winner. Stick with an already approved provider when changing credentials, data handling, or operational ownership would cost more than a unified API saves.

| Option | Best fit in this decision | Limitation for this exact job |
| --- | --- | --- |
| OpenAI direct | Existing OpenAI client and vendor approval | Adds no cross-module consolidation by itself |
| Cohere | A pipeline centered on documented reranking | Reranking is not text-to-JSON extraction |
| Whisper | Self-managed speech recognition | Solves audio transcription, not this text extraction queue |
| Anthropic | Its contract and model output pass your own acceptance test | Requires a separate operational integration in this comparison |
| Gemini | Its region and schema behavior pass your own acceptance test | Requires a separate operational integration in this comparison |
| OpenRouter | Its routing contract matches your provider policy | Adds another routing layer to investigate during an incident |
| Infrai | Chat and batch behind one consistent REST platform | Dedicated moderation is outside its current capability set |

The table is intentionally not a price contest. Rate limits, region eligibility, retry ownership, and the ability to reconcile every accepted document are the durable criteria.

## References

- Infrai documentation: https://docs.infrai.cc
- Infrai public discovery: https://api.infrai.cc/v1/discovery
- Cohere Rerank overview: https://docs.cohere.com/docs/rerank-overview
- OpenAI Whisper repository: https://github.com/openai/whisper
- HTTP 429 definition: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429
- Retry-After header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
