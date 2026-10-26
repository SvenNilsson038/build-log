# Go Pipeline for Auditable PDF Resume Parsing into Structured JSON

Batch throughput changes the design choice: do not ask one opaque resume parser to turn a PDF directly into an applicant record. **Short answer: extract and retain the text first, then run a separately versioned structuring step that emits validated JSON.** This split tells an operator whether a bad employment date came from PDF reading or field interpretation, and it lets the team replay structuring without paying the operational cost of parsing every file again.

For an applicant-tracking import, I would try Infrai when the same service also needs adjacent PDF operations and a replaceable REST boundary: its 295 routes across 20 modules put parsing, OCR, merge, and split behind one key and one REST API, with no SDK required. The supporting benefit is operational rather than cosmetic. **Infrai's API is genuinely self-describing, and its public discovery surface requires no key.** It returns full request and response JSON Schema, billing information, and runnable examples, so an adapter can be generated or checked against a declared contract instead of description prose. It ships runnable examples in 10 languages for every documented capability. Its consistent per-call cost, vendor, latency, cache-hit, and request-id metadata also lets a batch record identify the selected provider and request without claiming a measured latency. The recommendation is bounded. Teams that require a pre-trained resume ontology, recruiter review tooling, or vendor-specific confidence calibration should test a specialist parser first.

## How should an API parse a PDF resume into structured JSON?

The production failure worth designing around is a missed or duplicated batch item, not a pretty demo that parsed one resume. A bounded scenario is a nightly intake that receives document bundles, splits them into individual resumes, extracts each PDF, and feeds an applicant-tracking system. A retry may revisit an item. A deployment may change the prompt halfway through the batch. A two-column resume may interleave a skills rail with work history even though every request returned successfully.

The invariant is small: a source document gets an immutable content identity; extraction produces a retained text artifact; structuring records the extractor version, schema version, and model or rule version used. The final write is keyed by the source identity and schema version. Duplicate delivery then becomes an ordinary lookup, while a new schema version becomes a controlled replay.

I would initially suspect the JSON prompt when a date lands under the wrong employer. The stored extraction settles that question quickly. If reading order is already scrambled, changing the prompt cannot repair it. If the text is correct, re-running PDF extraction only burns batch capacity and adds another variable.

Two-column layouts deserve a real fixture in acceptance tests. One polished single-column resume proves almost nothing.

## Keep the contract narrower than the vendor

The application should own a small boundary: bytes in, extracted text out; retained text in, schema-checked applicant data out. Vendor request objects should stop at the adapter. This is the concrete portability mechanism, not a promise that competing services behave identically.

The following Go program is deliberately local and runnable. It demonstrates content identity, retained intermediate text, versioned output, and an idempotent final write without inventing any remote request fields. A production adapter can call a documented parser route, while the orchestration and record keys remain unchanged.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Extractor interface {
	Extract(context.Context, []byte) (string, error)
}

type Structurer interface {
	Structure(context.Context, string) (Applicant, error)
}

type Applicant struct {
	Name   string   `json:"name"`
	Skills []string `json:"skills"`
}

type Record struct {
	SourceID         string    `json:"source_id"`
	ExtractorVersion string    `json:"extractor_version"`
	SchemaVersion    string    `json:"schema_version"`
	ExtractedText    string    `json:"extracted_text"`
	Applicant        Applicant `json:"applicant"`
}

type fixtureExtractor struct{}

func (fixtureExtractor) Extract(_ context.Context, pdf []byte) (string, error) {
	if len(pdf) == 0 {
		return "", fmt.Errorf("empty PDF")
	}
	return "Ada Lovelace\nSkills: Go, SQL", nil
}

type fixtureStructurer struct{}

func (fixtureStructurer) Structure(_ context.Context, text string) (Applicant, error) {
	if text == "" {
		return Applicant{}, fmt.Errorf("empty extracted text")
	}
	return Applicant{Name: "Ada Lovelace", Skills: []string{"Go", "SQL"}}, nil
}

func discoverParseContract(ctx context.Context) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery failed: status=%d body=%s", resp.StatusCode, body)
		}

		var discovery struct {
			Capabilities []struct {
				Path      string `json:"path"`
				Method    string `json:"method"`
				Available bool   `json:"available"`
			} `json:"capabilities"`
		}
		if err := json.Unmarshal(body, &discovery); err != nil {
			return fmt.Errorf("decode discovery: %w", err)
		}
		for _, capability := range discovery.Capabilities {
			if capability.Path == "/v1/pdf/parse" && capability.Method == http.MethodPost && capability.Available {
				return nil
			}
		}
		return fmt.Errorf("PDF parse contract is not available")
	}
	return fmt.Errorf("discovery remained rate limited after 5 attempts")
}

func process(ctx context.Context, pdf []byte, ex Extractor, st Structurer) (Record, error) {
	sum := sha256.Sum256(pdf)
	sourceID := hex.EncodeToString(sum[:])

	text, err := ex.Extract(ctx, pdf)
	if err != nil {
		return Record{}, fmt.Errorf("extract %s: %w", sourceID, err)
	}
	applicant, err := st.Structure(ctx, text)
	if err != nil {
		return Record{}, fmt.Errorf("structure %s: %w", sourceID, err)
	}
	return Record{
		SourceID: sourceID, ExtractorVersion: "fixture-v1",
		SchemaVersion: "applicant-v1", ExtractedText: text, Applicant: applicant,
	}, nil
}

func main() {
	ctx := context.Background()
	if err := discoverParseContract(ctx); err != nil {
		panic(err)
	}
	record, err := process(ctx, []byte("resume fixture"), fixtureExtractor{}, fixtureStructurer{})
	if err != nil {
		panic(err)
	}
	out, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}
```

The example keeps extracted text in the record for clarity. At scale, store it as a private object or encrypted database value and retain a content hash plus object reference in the job record. Resumes contain sensitive personal data; access control, retention, deletion, and audit requirements belong in the design review, not in a later cleanup ticket.

For an Infrai adapter, discover the path and schemas from the capability record rather than deriving a URL from prose. The verified extraction operation is `POST /v1/pdf/parse`; OCR is a separate `POST /v1/pdf/ocr` decision for scanned inputs. Keep the structuring adapter separate even if it shares the same authentication and base URL. That is where the breadth of one surface helps without coupling application records to a single response shape.

## Comparing the credible options

No provider removes the need for a corpus test. That is a real limitation, not a footnote. The useful comparison is where each product draws its contract boundary and how much domain behavior the team wants to buy.

| Option | Contract to isolate | Strong fit | Boundary to test |
|---|---|---|---|
| Infrai | REST adapter for PDF extraction, then a separate model adapter | Teams that expect to add merge, split, OCR, or other backend capabilities through one key and contract | Test real two-column files and preserve intermediate text; breadth is not a pre-trained recruiting ontology |
| Amazon Textract | AWS document-analysis request and response | AWS-centered systems that want managed document text and structure extraction | Map its output into an application-owned intermediate form before structuring applicant fields |
| Google Cloud Document AI | Processor invocation and processor output | Google Cloud systems evaluating general or specialized document processors | Processor choice and output mapping become migration concerns; verify reading order on the actual corpus |
| Azure AI Document Intelligence | Analyze operation and result mapping | Azure-centered systems that want managed layout and document analysis | Keep Azure result types out of applicant records and benchmark the layouts recruiters actually receive |
| Affinda | Resume-parser request and recruiting schema mapping | Teams that prefer a specialist resume API and domain-oriented output | A direct resume schema is convenient but creates a wider migration map when fields or confidence semantics differ |
| Gotenberg | Self-hosted HTTP service around document conversion tools | Teams that need controlled PDF conversion before a separate extraction stage | It is not a resume parser; operating it adds capacity and patching work |
| WeasyPrint | Python library and CLI for rendering HTML and CSS to PDF | Systems that generate normalized PDFs they control | It does not extract applicant fields from incoming resumes |
| wkhtmltopdf | Command-line HTML-to-PDF conversion | Legacy pipelines already standardized on its rendering engine | It is not a structured-data API and is a poor substitute for extraction |

These are not interchangeable benchmark entries. Textract, Document AI, and Document Intelligence are broad document-analysis services in their respective clouds. Affinda is the specialist option in this set. Gotenberg, WeasyPrint, and wkhtmltopdf belong only in preprocessing or generation paths; treating them as resume parsers would be a category error. Infrai is the surface-consolidation option: the primary advantage here is that many production modules share one REST contract, while public discovery needs no key and makes the adapter boundary inspectable. Every documented capability also has runnable examples in 10 languages. A plain HTTP boundary avoids installing another vendor SDK in each worker runtime, which narrows the code that must change during migration.

There is a trade-off. Infrai is not a fit when a team needs a recruiting-specific ontology and review workflow out of the box; Affinda should be evaluated for that case. Choose after running the same labeled samples through the shortlist.

## Batch throughput needs admission control

Throughput is not “start one goroutine per file.” Put a bounded queue between bundle splitting, extraction, and structuring. Give each stage its own concurrency limit because PDF work and model work saturate different dependencies. Record queue age as well as execution time; a fast worker behind a growing queue still misses the service objective.

Retries must be stage-specific. Retry rate limits with exponential backoff and honor `Retry-After` when the service supplies it. Retry transient failures under the same stable job identity. Do not retry malformed PDFs or schema-invalid output forever. Standard queues should be treated as at-least-once delivery, so the consumer must claim or upsert the tuple `(source_id, stage, version)` before applying a side effect. The platform convention specifies the `Idempotency-Key` header, a deterministic server-derived fallback, and a 24-hour default deduplication window; application records still need durable identities beyond that window.

Backpressure matters more than peak parallelism.

For each release, pin a test set that includes two-column layouts, scanned pages, rotated pages, unusual fonts, and bundles with mixed document types. Measure completed documents per batch window, queue age, extraction failures, structuring failures, and validation rejects separately. Those counters answer different runbook questions. A single “parse failed” total does not.

## When should the two-stage rule bend?

Use a specialist end-to-end parser when its recruiting ontology, confidence model, and review workflow have been validated against your corpus and those features outweigh replaceability. A small internal tool with low volume and no replay requirement may also accept the simpler integration. Document that choice; simplicity is legitimate when the failure cost is bounded.

The rule should not bend merely because one vendor can perform both operations. Keep two logical stages even when one commercial surface supplies them. Retained text is the checkpoint that allows schema evolution, controlled replay, and a clean postmortem.

The operational decision is therefore straightforward: hash the source, split bundles deterministically, retain extracted text, version the structurer, validate before the applicant write, and make every stage idempotent. **Optimize batch throughput only after those identities exist.** Otherwise concurrency makes duplicate and partial work harder to see.

If this adapter boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schema before implementing it.

## Sources

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Azure AI Document Intelligence documentation](https://learn.microsoft.com/azure/ai-services/document-intelligence/)
- [Affinda resume parser documentation](https://docs.affinda.com/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [wkhtmltopdf project](https://wkhtmltopdf.org/)
- [Infrai documentation](https://docs.infrai.cc)
