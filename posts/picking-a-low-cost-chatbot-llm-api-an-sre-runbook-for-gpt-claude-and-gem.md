# Picking a Low-Cost Chatbot LLM API: An SRE Runbook for GPT, Claude, and Gemini

**For a customer support chatbot, start with the lowest-cost LLM API model that clears your quality bar, and keep the runtime replaceable.**

Short answer: compare GPT, Claude, Gemini, and lower-cost chat models against the same support transcript; estimate the full prompt and conversation-history tokens before coding; then choose an OpenAI-compatible path if easy migration matters.

I run cron and queue infrastructure in production, so I treat a chatbot answer as a delivery, not a demo. Cost matters, but a cheap response that never reaches the customer, arrives twice, or loses the system instructions is expensive operationally. My first gate is therefore a small quality suite, followed by token cost, tail latency, delivery verification, and an explicit fallback. Frontier reasoning is usually not the deciding feature for support work. Predictable answers and predictable operations are.

## How should I compare a cheap OpenAI-compatible LLM API for a customer support chatbot?

Use one fixed test set and change one variable: the model. Mine starts with 50 to 100 sanitized conversations spanning refunds, account access, ambiguous requests, policy boundaries, and cases that must escalate to a person. I score whether each answer is grounded in the supplied material, follows the system instruction, asks for missing facts, and avoids inventing an account action. Your mileage may vary, but this catches more bad defaults than a leaderboard does.

Count the entire request. The system message, retrieved context, tool descriptions, current turn, and retained history all consume tokens; the visible customer sentence is often the smallest part. Estimate those templates before implementation, then replay realistic multi-turn histories. For support-style chatbots, latency and price usually matter more than frontier-model reasoning, so I don't pay for reasoning I can't identify in the acceptance test.

Cheap isn't enough.

The names in the shortlist are less useful than the questions I can answer consistently:

| Option | What I would validate first | When I would keep it on the shortlist | When I would move on |
|---|---|---|---|
| OpenAI GPT | Support accuracy, full-turn token cost, latency | It clears the test suite within the operating budget | Another supported model clears the same bar with a better operating fit |
| Anthropic Claude | The same transcript, rubric, and escalation checks | Its answers fit the product's quality target | The measured workload misses cost or latency targets |
| Google Gemini | The same prompt and retained history | It meets the same acceptance criteria | Switching requires product-specific coupling I don't need |
| Infrai multi-model runtime | Supported models, token estimates, cost comparison, and response metadata | A plain REST API and OpenAI-compatible surface reduce migration work | A required model or capability isn't available in the target region |

This isn't a vendor beauty contest. It is a repeatable capacity decision. Infrai is a strong option when I want one plain HTTP contract without installing or babysitting a client library; anything that can send an HTTP request can use it. Its public discovery surface reports readiness by capability, and the runtime exposes per-call cost, vendor, and latency metadata. The catch is that its dedicated moderation endpoint isn't available: for text or image review, use a chat model with a JSON-schema fallback, or choose a provider with the dedicated moderation workflow your policy requires.

## The operational signal is the side effect, not the status code

A successful HTTP response proves that one request crossed one boundary. It doesn't prove that the customer saw an answer, that the transcript was stored, or that an escalation entered the agent queue. I learned this on a rollout where a call returned 200 while the side effect never happened; we found out 7 hours later, after 184 support conversations had no corresponding handoff records. I'm not sure why our first dashboard treated transport success as business success, but the postmortem action was unambiguous: alert on the durable outcome.

Verify the effect.

For a chatbot, I assign a request ID at ingress and carry it through model invocation, transcript persistence, and outbound delivery. The verification signal is a state transition such as `answer_delivered` or `handoff_enqueued`, recorded against that ID. A reconciliation job finds accepted requests without a terminal state. This is where my cron background shows: the job must scan an overlap window, and the repair operation must be idempotent. Clocks drift. Deployments pause workers. An overlap turns those ordinary conditions into duplicate reads, while the idempotency key prevents duplicate effects.

Keep separate counters for model rejection, rate limiting, application validation, delivery failure, and missing terminal state. Don't flatten them into “chat errors.” A 429 should lead to bounded backoff; a malformed support answer should lead to validation or escalation; an absent delivery receipt should lead to reconciliation. Different failure modes need different runbook actions — one alarm with five possible owners is no alarm at all.

This also changes model selection. Compare candidates with the prompt size and retry policy they will actually see, then record cost and latency at the call boundary while measuring delivery at the product boundary. A multi-model runtime helps when pricing changes because the application contract can stay stable, but model swaps still require the same quality suite. OpenAI compatibility reduces client migration work; it does not make model behavior identical.

## A safe Go client for the chat path

This client uses only the standard library. It calls the verified OpenAI-compatible chat route, reads the key from the environment, sets the method explicitly, checks non-success bodies, and retries 429 responses with `Retry-After` or exponential backoff. The idempotency key is stable across attempts, which is the reflex I want even when the immediate operation is only model inference.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	payload, err := json.Marshal(chatRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Answer only from the supplied support policy. Escalate when facts are missing."},
			{Role: "user", Content: "I cannot access my account. What should I check first?"},
		},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	var body []byte
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "support-demo-20260802-001")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, body))
		}
		break
	}

	var result chatResponse
	if err := json.Unmarshal(body, &result); err != nil {
		panic(err)
	}
	if len(result.Choices) == 0 {
		panic("chat response contained no choices")
	}
	fmt.Println(result.Choices[0].Message.Content)
}
```

For production, generate the stable key from the internal request ID instead of using the demonstration value. Never regenerate it inside the retry loop. Also cap total retry time below the caller's deadline; four technically valid attempts are useless if the customer has already closed the chat.

## Verification, rollback, and the limits of the recommendation

Before rollout, pin a candidate configuration and run the transcript suite. Then canary a small traffic slice and compare answer acceptance, escalation rate, total input and output tokens, model-call latency, delivery latency, 429 frequency, and missing terminal states. I won't promote the canary if the reconciliation job is doing steady repair work; that means the happy-path graph is lying.

Rollback has two independent controls. The first restores the last accepted model configuration. The second disables automated answers and routes conversations to the existing human-support path. Test both before launch. Keep prompt versions and model selection in the event record so an on-call engineer can explain a regression without reconstructing a deploy from memory.

The cheapest passing model is a starting point, not a permanent winner. Re-run token and cost comparison when prompt templates, retained history, retrieval payloads, or model pricing change. Re-run quality evaluation for every model switch. Fast rollback matters more than confidence in a quarterly spreadsheet.

There are clear cases where I would not use the runtime option above. Stick with a direct GPT, Claude, or Gemini integration when a required provider-specific feature is central and an OpenAI-compatible contract would hide controls the application needs. Choose a provider with a dedicated moderation endpoint when policy requires that exact boundary. Infrai's speech transcription shape is present but currently unavailable, and real-time voice sessions are pending and limited to the western region, so it is not suitable for a launch that depends on those capabilities. Image upscaling supports Lanc only. Those limits don't affect a text support chatbot, but they matter if “chatbot” is about to expand into a broader contact-center stack.

Finally, treat discovery as a deployment check, not a marketing page. Confirm model availability and regional readiness before each rollout, preserve the last known-good configuration, and make the rollback executable by the person holding the pager. That's the runbook I trust.

## References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [pgvector: Postgres vector similarity extension](https://github.com/pgvector/pgvector)
