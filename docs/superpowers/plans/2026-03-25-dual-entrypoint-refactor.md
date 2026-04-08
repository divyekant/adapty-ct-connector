# Dual Entrypoint Refactor (Lambda + Fargate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract shared processing logic so the connector can run as either a Lambda (SQS event source) or a Fargate task (SQS long-poll), selected at deploy time.

**Architecture:** Create `internal/processor` package that owns parse-transform-upload pipeline. Both `cmd/connector` (Fargate) and `cmd/lambda` (Lambda) call the processor, differing only in how they receive SQS messages and report outcomes. Lambda uses `provided.al2023` runtime with a compiled Go binary (no containers).

**Tech Stack:** Go 1.25, `github.com/aws/aws-lambda-go`, AWS SQS event source mapping, existing transform + CleverTap packages.

---

## File Structure

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `internal/processor/processor.go` | Uploader interface, Processor struct, Process method (parse → dedup → transform → upload → classify outcomes) |
| Create | `internal/processor/processor_test.go` | Unit tests for processor with mock uploader |
| Modify | `internal/queue/consumer.go` | Remove processing logic; delegate to processor, keep SQS polling + message deletion |
| Modify | `internal/queue/consumer_test.go` | Update tests for new delegation model |
| Create | `cmd/lambda/main.go` | Lambda handler: SQS event → processor → SQSBatchResponse |
| Create | `Makefile` | Build targets for connector, lambda, backfill |
| Modify | `go.mod` / `go.sum` | Add `github.com/aws/aws-lambda-go` |
| Modify | `docs/architecture.md` | Add Lambda deployment option alongside Fargate |

---

### Task 1: Create `internal/processor` package — types and interface

**Files:**
- Create: `internal/processor/processor.go`

- [ ] **Step 1: Create the processor package with types and constructor**

```go
package processor

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

// Uploader abstracts the CleverTap upload operation.
type Uploader interface {
	Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error)
}

// InputMessage is a single raw message to process.
type InputMessage struct {
	ID   string // SQS message ID (used for batch failure reporting)
	Body []byte // Raw JSON body
}

// Outcome classifies the processing result for a single message.
type Outcome int

const (
	OutcomeSuccess Outcome = iota // Processed and uploaded — acknowledge (delete)
	OutcomeSkip                   // Not an error but nothing to upload (dedup, verification) — acknowledge (delete)
	OutcomeFail                   // Error — do not acknowledge (retry)
)

// MessageResult holds the outcome for one input message.
type MessageResult struct {
	Index     int
	MessageID string
	Outcome   Outcome
	EventType string // For logging; empty if parse failed
	Identity  string // For logging; empty if parse failed
	EventID   string // profile_event_id for logging
	Error     error  // Non-nil for OutcomeFail
}

// BatchResult holds the outcomes of processing a batch.
// Results is indexed identically to the input messages slice: Results[i] corresponds to messages[i].
type BatchResult struct {
	Results    []MessageResult
	FatalError error // Non-nil for auth errors — caller should stop
}

// Processor owns the parse → dedup → transform → upload pipeline.
type Processor struct {
	uploader Uploader
	cfg      *transform.Config
	dedup    *lru.Cache[string, struct{}]
}

// New creates a Processor. Pass dedupSize <= 0 to disable deduplication (e.g. for Lambda).
func New(uploader Uploader, cfg *transform.Config, dedupSize int) *Processor {
	var cache *lru.Cache[string, struct{}]
	if dedupSize > 0 {
		var err error
		cache, err = lru.New[string, struct{}](dedupSize)
		if err != nil {
			panic("processor: invalid dedupSize")
		}
	}
	return &Processor{
		uploader: uploader,
		cfg:      cfg,
		dedup:    cache,
	}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd /Users/dk/projects/adapty-ct-connector && go build ./internal/processor/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/processor/processor.go
git commit -m "feat: add processor package with types and constructor"
```

---

### Task 2: Implement `Processor.Process` method

**Files:**
- Modify: `internal/processor/processor.go`
- Create: `internal/processor/processor_test.go`

- [ ] **Step 1: Write failing tests for Process**

Create `internal/processor/processor_test.go`:

```go
package processor

import (
	"encoding/json"
	"testing"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

type mockUploader struct {
	uploaded []clevertap.UploadRequest
	response *clevertap.UploadResponse
	err      error
}

func (m *mockUploader) Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error) {
	m.uploaded = append(m.uploaded, req)
	if m.err != nil {
		return nil, m.err
	}
	if m.response != nil {
		return m.response, nil
	}
	return &clevertap.UploadResponse{Status: "success", Processed: len(req.D)}, nil
}

func makeMessage(id, eventType, profileEventID string) InputMessage {
	evt := adapty.Event{
		ProfileID:     "profile-" + id,
		EventType:     eventType,
		EventDatetime: "2024-01-15T10:00:00.000000+0000",
		EventProperties: map[string]interface{}{
			"profile_event_id": profileEventID,
		},
	}
	b, _ := json.Marshal(evt)
	return InputMessage{ID: "msg-" + id, Body: b}
}

func TestProcess_HappyPath(t *testing.T) {
	ct := &mockUploader{}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{
		makeMessage("1", "subscription_started", "evt-001"),
		makeMessage("2", "trial_started", "evt-002"),
	}

	result := p.Process(msgs)
	if result.FatalError != nil {
		t.Fatalf("unexpected fatal error: %v", result.FatalError)
	}
	if len(result.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.Results))
	}
	for i, r := range result.Results {
		if r.Outcome != OutcomeSuccess {
			t.Errorf("result[%d]: expected OutcomeSuccess, got %d (err=%v)", i, r.Outcome, r.Error)
		}
	}
	if len(ct.uploaded) != 1 {
		t.Errorf("expected 1 upload call, got %d", len(ct.uploaded))
	}
	if len(ct.uploaded[0].D) != 2 {
		t.Errorf("expected 2 records in upload, got %d", len(ct.uploaded[0].D))
	}
}

func TestProcess_SkipsEmptyEventType(t *testing.T) {
	ct := &mockUploader{}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	empty, _ := json.Marshal(map[string]interface{}{})
	msgs := []InputMessage{{ID: "msg-v", Body: empty}}

	result := p.Process(msgs)
	if result.FatalError != nil {
		t.Fatalf("unexpected fatal error: %v", result.FatalError)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0].Outcome != OutcomeSkip {
		t.Errorf("expected OutcomeSkip, got %d", result.Results[0].Outcome)
	}
	if len(ct.uploaded) != 0 {
		t.Errorf("expected 0 upload calls, got %d", len(ct.uploaded))
	}
}

func TestProcess_DeduplicatesWithCache(t *testing.T) {
	ct := &mockUploader{}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 1000) // dedup enabled

	msg := makeMessage("1", "subscription_started", "evt-dup")
	// Same body but different message IDs (simulates SQS redelivery)
	msg2 := InputMessage{ID: "msg-2", Body: msg.Body}

	result := p.Process([]InputMessage{msg, msg2})
	if result.FatalError != nil {
		t.Fatalf("unexpected fatal error: %v", result.FatalError)
	}
	if len(result.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.Results))
	}
	if result.Results[0].Outcome != OutcomeSuccess {
		t.Errorf("first message should succeed, got %d", result.Results[0].Outcome)
	}
	if result.Results[1].Outcome != OutcomeSkip {
		t.Errorf("second message should be skipped as duplicate, got %d", result.Results[1].Outcome)
	}
	// Only 1 record should be uploaded
	if len(ct.uploaded) != 1 || len(ct.uploaded[0].D) != 1 {
		t.Errorf("expected 1 upload with 1 record")
	}
}

func TestProcess_MalformedJSON(t *testing.T) {
	ct := &mockUploader{}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{{ID: "msg-bad", Body: []byte("not json")}}

	result := p.Process(msgs)
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0].Outcome != OutcomeFail {
		t.Errorf("expected OutcomeFail for malformed JSON, got %d", result.Results[0].Outcome)
	}
}

func TestProcess_AuthError(t *testing.T) {
	ct := &mockUploader{err: &clevertap.AuthError{StatusCode: 401}}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{makeMessage("1", "subscription_started", "evt-001")}

	result := p.Process(msgs)
	if result.FatalError == nil {
		t.Fatal("expected FatalError for auth failure")
	}
	// All results should be failures
	for _, r := range result.Results {
		if r.Outcome != OutcomeFail {
			t.Errorf("expected OutcomeFail on auth error, got %d", r.Outcome)
		}
	}
}

func TestProcess_PartialUploadFailure(t *testing.T) {
	ct := &mockUploader{
		response: &clevertap.UploadResponse{
			Status:    "partial",
			Processed: 1,
			Unprocessed: []clevertap.Unprocessed{
				{Status: "fail", Code: 1, Error: "invalid identity", Record: 1},
			},
		},
	}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{
		makeMessage("1", "subscription_started", "evt-001"),
		makeMessage("2", "trial_started", "evt-002"),
	}

	result := p.Process(msgs)
	if result.FatalError != nil {
		t.Fatalf("unexpected fatal error: %v", result.FatalError)
	}
	if result.Results[0].Outcome != OutcomeSuccess {
		t.Errorf("first message should succeed, got %d", result.Results[0].Outcome)
	}
	if result.Results[1].Outcome != OutcomeFail {
		t.Errorf("second message should fail (partial), got %d", result.Results[1].Outcome)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/dk/projects/adapty-ct-connector && go test ./internal/processor/ -v`
Expected: compilation error — `Process` method not defined yet

- [ ] **Step 3: Implement the Process method**

Add to `internal/processor/processor.go`:

```go
// Process takes a batch of raw messages, transforms them, uploads to CleverTap,
// and returns per-message outcomes. The caller is responsible for SQS acknowledgment
// (deleting messages for OutcomeSuccess/OutcomeSkip, leaving OutcomeFail for retry).
func (p *Processor) Process(messages []InputMessage) *BatchResult {
	results := make([]MessageResult, len(messages))
	for i, m := range messages {
		results[i] = MessageResult{Index: i, MessageID: m.ID}
	}

	type item struct {
		index   int
		record  clevertap.EventRecord
		eventID string
	}
	var items []item

	for i, msg := range messages {
		var evt adapty.Event
		if err := json.Unmarshal(msg.Body, &evt); err != nil {
			slog.Error("processor: malformed message",
				"message_id", msg.ID, "err", err)
			results[i].Outcome = OutcomeFail
			results[i].Error = err
			continue
		}

		results[i].EventType = evt.EventType

		if evt.EventType == "" {
			slog.Debug("processor: empty event_type (verification), skipping",
				"message_id", msg.ID)
			results[i].Outcome = OutcomeSkip
			continue
		}

		eventID := getEventID(evt)
		results[i].EventID = eventID

		if eventID != "" && p.dedup != nil {
			if p.dedup.Contains(eventID) {
				slog.Debug("processor: duplicate event, skipping",
					"profile_event_id", eventID, "message_id", msg.ID)
				results[i].Outcome = OutcomeSkip
				continue
			}
		}

		record, err := transform.Transform(evt, p.cfg)
		if err != nil {
			slog.Error("processor: transform error",
				"message_id", msg.ID, "event_type", evt.EventType, "err", err)
			results[i].Outcome = OutcomeFail
			results[i].Error = err
			continue
		}

		results[i].Identity = record.Identity

		if eventID != "" && p.dedup != nil {
			p.dedup.Add(eventID, struct{}{})
		}

		items = append(items, item{index: i, record: record, eventID: eventID})
	}

	if len(items) == 0 {
		return &BatchResult{Results: results}
	}

	records := make([]clevertap.EventRecord, len(items))
	for i, it := range items {
		records[i] = it.record
	}

	uploadStart := time.Now()
	resp, err := p.uploader.Upload(clevertap.UploadRequest{D: records})
	if err != nil {
		var authErr *clevertap.AuthError
		if errors.As(err, &authErr) {
			slog.Error("processor: CleverTap authentication failure", "err", err)
			for _, it := range items {
				results[it.index].Outcome = OutcomeFail
				results[it.index].Error = err
			}
			return &BatchResult{Results: results, FatalError: err}
		}
		slog.Error("processor: CleverTap upload error", "err", err)
		for _, it := range items {
			results[it.index].Outcome = OutcomeFail
			results[it.index].Error = err
		}
		return &BatchResult{Results: results}
	}

	failedIdx := make(map[int]clevertap.Unprocessed, len(resp.Unprocessed))
	for _, u := range resp.Unprocessed {
		failedIdx[u.Record] = u
	}

	latencyMs := time.Since(uploadStart).Milliseconds()
	for uploadIdx, it := range items {
		if u, failed := failedIdx[uploadIdx]; failed {
			slog.Warn("processor: event not processed by CleverTap",
				"event_type", results[it.index].EventType,
				"identity", results[it.index].Identity,
				"profile_event_id", it.eventID,
				"status", u.Status,
				"code", u.Code,
				"error", u.Error,
				"latency_ms", latencyMs,
			)
			results[it.index].Outcome = OutcomeFail
			results[it.index].Error = errors.New(u.Error)
			continue
		}

		slog.Info("processor: event processed",
			"event_type", results[it.index].EventType,
			"identity", results[it.index].Identity,
			"profile_event_id", it.eventID,
			"status", clevertap.StatusSuccess,
			"latency_ms", latencyMs,
		)
		results[it.index].Outcome = OutcomeSuccess
	}

	return &BatchResult{Results: results}
}

// getEventID extracts profile_event_id from event_properties.
func getEventID(evt adapty.Event) string {
	if evt.EventProperties == nil {
		return ""
	}
	v, ok := evt.EventProperties["profile_event_id"]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dk/projects/adapty-ct-connector && go test ./internal/processor/ -v`
Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/processor/processor.go internal/processor/processor_test.go
git commit -m "feat: implement processor.Process with parse/dedup/transform/upload pipeline"
```

---

### Task 3: Refactor consumer to delegate to processor

**Files:**
- Modify: `internal/queue/consumer.go`
- Modify: `internal/queue/consumer_test.go`

- [ ] **Step 1: Rewrite consumer.go to delegate to processor**

Replace `internal/queue/consumer.go` with:

```go
package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/processor"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

// Message represents a single SQS message.
type Message struct {
	Body          string
	ReceiptHandle string
	MessageID     string
}

// SQSClient abstracts SQS operations required by the consumer.
type SQSClient interface {
	ReceiveMessages(ctx context.Context, maxMessages int) ([]Message, error)
	DeleteMessage(ctx context.Context, receiptHandle string) error
}

const sqsMaxMessages = 10

type Consumer struct {
	sqs       SQSClient
	proc      *processor.Processor
	batchSize int
}

// NewConsumer creates a Consumer that polls SQS and delegates processing to the shared Processor.
func NewConsumer(sqs SQSClient, uploader processor.Uploader, cfg *transform.Config, batchSize, dedupSize int) *Consumer {
	if batchSize > sqsMaxMessages {
		batchSize = sqsMaxMessages
	}
	return &Consumer{
		sqs:       sqs,
		proc:      processor.New(uploader, cfg, dedupSize),
		batchSize: batchSize,
	}
}

// Run starts the polling loop. It calls ProcessBatch in a tight loop, calls onLoop after each
// iteration, and sleeps 1 second when the queue is empty.
func (c *Consumer) Run(ctx context.Context, onLoop func()) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("queue: consumer shutting down")
			return
		default:
		}

		processed, err := c.ProcessBatch(ctx)
		if err != nil {
			slog.Error("queue: ProcessBatch error", "err", err)
		}

		if onLoop != nil {
			onLoop()
		}

		if processed == 0 {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				slog.Info("queue: consumer shutting down")
				return
			case <-timer.C:
			}
		}
	}
}

// ProcessBatch receives messages from SQS, delegates to the processor, and
// deletes messages that were successfully processed or skipped.
func (c *Consumer) ProcessBatch(ctx context.Context) (int, error) {
	msgs, err := c.sqs.ReceiveMessages(ctx, c.batchSize)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	// Convert to processor input
	input := make([]processor.InputMessage, len(msgs))
	for i, m := range msgs {
		input[i] = processor.InputMessage{
			ID:   m.MessageID,
			Body: []byte(m.Body),
		}
	}

	result := c.proc.Process(input)

	if result.FatalError != nil {
		return 0, result.FatalError
	}

	successCount := 0
	for i, r := range result.Results {
		switch r.Outcome {
		case processor.OutcomeSuccess, processor.OutcomeSkip:
			if delErr := c.sqs.DeleteMessage(ctx, msgs[i].ReceiptHandle); delErr != nil {
				slog.Error("queue: delete message", "message_id", msgs[i].MessageID, "err", delErr)
				continue
			}
			if r.Outcome == processor.OutcomeSuccess {
				successCount++
			}
		case processor.OutcomeFail:
			// Leave in SQS for retry
		}
	}

	return successCount, nil
}
```

- [ ] **Step 2: Update consumer_test.go**

The existing test mock `mockCT` implements `Upload` — it now needs to satisfy `processor.Uploader`. Since `processor.Uploader` has the same method signature as the old `CTUploader`, the mock works as-is. Update the import and `NewConsumer` call:

Replace `internal/queue/consumer_test.go` with:

```go
package queue

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

// --- mocks ---

type mockSQS struct {
	messages []string
	deleted  []string
}

func (m *mockSQS) ReceiveMessages(_ context.Context, maxMessages int) ([]Message, error) {
	if len(m.messages) == 0 {
		return nil, nil
	}
	n := maxMessages
	if n > len(m.messages) {
		n = len(m.messages)
	}
	batch := m.messages[:n]
	m.messages = m.messages[n:]

	msgs := make([]Message, len(batch))
	for i, b := range batch {
		msgs[i] = Message{
			Body:          b,
			ReceiptHandle: "rh-" + b[:8],
			MessageID:     "id-" + b[:8],
		}
	}
	return msgs, nil
}

func (m *mockSQS) DeleteMessage(_ context.Context, receiptHandle string) error {
	m.deleted = append(m.deleted, receiptHandle)
	return nil
}

type mockCT struct {
	uploaded []clevertap.UploadRequest
	response *clevertap.UploadResponse
	err      error
}

func (m *mockCT) Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error) {
	m.uploaded = append(m.uploaded, req)
	if m.err != nil {
		return nil, m.err
	}
	if m.response != nil {
		return m.response, nil
	}
	return &clevertap.UploadResponse{Status: "success", Processed: len(req.D)}, nil
}

func makeTestMessage(eventType, profileID string) string {
	evt := adapty.Event{
		ProfileID:     "profile-" + profileID,
		EventType:     eventType,
		EventDatetime: "2024-01-15T10:00:00.000000+0000",
		EventProperties: map[string]interface{}{
			"profile_event_id": profileID,
		},
	}
	b, _ := json.Marshal(evt)
	return string(b)
}

// --- tests ---

func TestConsumer_ProcessBatch(t *testing.T) {
	sqsMock := &mockSQS{
		messages: []string{
			makeTestMessage("subscription_started", "evt-001"),
			makeTestMessage("trial_started", "evt-002"),
		},
	}
	ctMock := &mockCT{}
	cfg := transform.DefaultConfig()
	c := NewConsumer(sqsMock, ctMock, cfg, 10, 1000)

	processed, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 2 {
		t.Errorf("expected 2 processed, got %d", processed)
	}
	if len(ctMock.uploaded) != 1 {
		t.Errorf("expected 1 upload call, got %d", len(ctMock.uploaded))
	}
	if len(ctMock.uploaded[0].D) != 2 {
		t.Errorf("expected 2 records in upload, got %d", len(ctMock.uploaded[0].D))
	}
	if len(sqsMock.deleted) != 2 {
		t.Errorf("expected 2 deleted messages, got %d", len(sqsMock.deleted))
	}
}

func TestConsumer_SkipsDuplicates(t *testing.T) {
	body := makeTestMessage("subscription_started", "evt-dup")
	sqsMock := &mockSQS{
		messages: []string{body, body},
	}
	ctMock := &mockCT{}
	cfg := transform.DefaultConfig()
	c := NewConsumer(sqsMock, ctMock, cfg, 10, 1000)

	processed, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 processed, got %d", processed)
	}
	// Both messages deleted: 1 success + 1 skip (duplicate)
	if len(sqsMock.deleted) != 2 {
		t.Errorf("expected 2 deletes (1 dup + 1 success), got %d", len(sqsMock.deleted))
	}
	if len(ctMock.uploaded) != 1 || len(ctMock.uploaded[0].D) != 1 {
		t.Errorf("expected 1 upload with 1 record")
	}
}

func TestConsumer_SkipsEmptyEventType(t *testing.T) {
	emptyMsg, _ := json.Marshal(map[string]interface{}{})
	sqsMock := &mockSQS{
		messages: []string{string(emptyMsg)},
	}
	ctMock := &mockCT{}
	cfg := transform.DefaultConfig()
	c := NewConsumer(sqsMock, ctMock, cfg, 10, 1000)

	processed, err := c.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 0 {
		t.Errorf("expected 0 processed, got %d", processed)
	}
	if len(sqsMock.deleted) != 1 {
		t.Errorf("expected 1 delete (verification request), got %d", len(sqsMock.deleted))
	}
	if len(ctMock.uploaded) != 0 {
		t.Errorf("expected 0 upload calls, got %d", len(ctMock.uploaded))
	}
}

func TestConsumer_GracefulShutdown(t *testing.T) {
	sqsMock := &mockSQS{}
	ctMock := &mockCT{}
	cfg := transform.DefaultConfig()
	c := NewConsumer(sqsMock, ctMock, cfg, 10, 1000)

	var loopCount atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		c.Run(ctx, func() { loopCount.Add(1) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not shut down within 3 seconds")
	}

	if loopCount.Load() == 0 {
		t.Error("expected at least one loop iteration before shutdown")
	}
}
```

- [ ] **Step 3: Run all tests**

Run: `cd /Users/dk/projects/adapty-ct-connector && go test ./internal/... -v`
Expected: all tests in `processor` and `queue` pass

- [ ] **Step 4: Commit**

```bash
git add internal/queue/consumer.go internal/queue/consumer_test.go
git commit -m "refactor: consumer delegates to processor for event processing"
```

---

### Task 4: Update `cmd/connector/main.go` for new imports

**Files:**
- Modify: `cmd/connector/main.go`

- [ ] **Step 1: Update the imports and dryRunUploader**

The `dryRunUploader` in `cmd/connector/main.go` currently satisfies `queue.CTUploader`. That interface no longer exists — it's now `processor.Uploader`. Update the import and the variable type:

Two changes in `cmd/connector/main.go`:

1. Add `processor` to the import block:

```go
import (
	// ... existing imports stay ...
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/processor"
	"github.com/anthropic/adapty-ct-connector/internal/queue"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)
```

2. Change the uploader variable type (line 56):

```go
// Old:
var uploader queue.CTUploader

// New:
var uploader processor.Uploader
```

The `queue` import is still needed for `queue.NewConsumer` and `queue.NewSQSAdapter`. The `dryRunUploader` struct's `Upload` method already matches `processor.Uploader` — no changes to the struct itself.

- [ ] **Step 2: Run the full test suite**

Run: `cd /Users/dk/projects/adapty-ct-connector && go test ./... -v`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add cmd/connector/main.go
git commit -m "refactor: connector uses processor.Uploader interface"
```

---

### Task 5: Create Lambda handler

**Files:**
- Create: `cmd/lambda/main.go`
- Modify: `go.mod` / `go.sum`

- [ ] **Step 1: Add aws-lambda-go dependency**

Run: `cd /Users/dk/projects/adapty-ct-connector && go get github.com/aws/aws-lambda-go@latest`

- [ ] **Step 2: Create cmd/lambda/main.go**

```go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/processor"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

var proc *processor.Processor

func init() {
	logLevel := new(slog.LevelVar)
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		logLevel.Set(slog.LevelInfo)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	accountID := requireEnv("CT_ACCOUNT_ID")
	passcode := requireEnv("CT_PASSCODE")
	ctRegion := requireEnv("CT_REGION")

	transformConfigPath := os.Getenv("TRANSFORM_CONFIG_PATH")
	ctBaseURL := os.Getenv("CT_BASE_URL")

	cfg, err := transform.LoadConfig(transformConfigPath)
	if err != nil {
		slog.Error("failed to load transform config", "path", transformConfigPath, "err", err)
		os.Exit(1)
	}

	var uploader processor.Uploader
	if ctBaseURL != "" {
		uploader = clevertap.NewClient(accountID, passcode, ctBaseURL)
	} else {
		uploader = clevertap.NewClientFromRegion(accountID, passcode, ctRegion)
	}

	// No dedup cache for Lambda — each invocation is a fresh container.
	// SQS visibility timeout handles retry semantics.
	proc = processor.New(uploader, cfg, 0)

	slog.Info("lambda: initialized",
		"ct_region", ctRegion,
		"transform_config", transformConfigPath,
	)
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	messages := make([]processor.InputMessage, len(sqsEvent.Records))
	for i, record := range sqsEvent.Records {
		messages[i] = processor.InputMessage{
			ID:   record.MessageId,
			Body: []byte(record.Body),
		}
	}

	result := proc.Process(messages)

	var failures []events.SQSBatchItemFailure
	for i, r := range result.Results {
		if r.Outcome == processor.OutcomeFail {
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: sqsEvent.Records[i].MessageId,
			})
		}
	}

	// On auth errors, all messages are already marked OutcomeFail above.
	// Return them as batch item failures (not a Go error) so SQS retries them
	// without Lambda reporting a function error — avoids misleading error metrics.
	// Messages will eventually hit DLQ after maxReceiveCount attempts.
	if result.FatalError != nil {
		slog.Error("lambda: fatal processing error — all messages will retry via SQS",
			"err", result.FatalError, "batch_size", len(sqsEvent.Records))
	}

	return events.SQSEventResponse{BatchItemFailures: failures}, nil
}

func main() {
	lambda.Start(handler)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd /Users/dk/projects/adapty-ct-connector && go build ./cmd/lambda/`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add cmd/lambda/main.go go.mod go.sum
git commit -m "feat: add Lambda handler for SQS event source"
```

---

### Task 6: Add Makefile with build targets

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Create the Makefile**

```makefile
.PHONY: build build-connector build-lambda build-backfill test clean

build: build-connector build-lambda build-backfill

build-connector:
	CGO_ENABLED=0 go build -o bin/connector ./cmd/connector

build-lambda:
	mkdir -p bin/lambda
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/lambda/bootstrap ./cmd/lambda
	cd bin/lambda && zip -j ../lambda.zip bootstrap

build-backfill:
	CGO_ENABLED=0 go build -o bin/backfill ./cmd/backfill

test:
	go test ./... -v

clean:
	rm -rf bin/
```

- [ ] **Step 2: Verify Makefile works**

Run: `cd /Users/dk/projects/adapty-ct-connector && make test && make build`
Expected: all tests pass, binaries produced in `bin/`

- [ ] **Step 3: Add `bin/` to .gitignore**

Append `bin/` to `.gitignore` (create if it doesn't exist).

- [ ] **Step 4: Commit**

```bash
git add Makefile .gitignore
git commit -m "feat: add Makefile with build targets for connector, lambda, and backfill"
```

---

### Task 7: Update architecture documentation

**Files:**
- Modify: `docs/architecture.md`

- [ ] **Step 1: Update the architecture overview diagram**

Replace the existing diagram block (lines 9-27) with a dual-mode version:

```
## Architecture Overview

Two deployment modes are supported. Choose one per account based on your operational preferences:

### Option A: Lambda (Recommended)

\```
Adapty Webhooks
    │
    ▼
API Gateway (POST /ingest/{ct_account_id})
    │  ── validates Authorization header
    │  ── routes to per-account SQS queue
    ▼
SQS Queue (per account)
    │  ── standard queue, 14-day retention
    │  ── DLQ after 5 failed processing attempts
    ▼
Lambda (SQS event source mapping, Go binary)
    │  ── auto-invoked on batch size or batch window
    │  ── transforms events
    │  ── uploads to CleverTap
    │  ── reports partial batch failures
    ▼
CleverTap Upload Events API
\```

**Advantages:** No container pipeline, scales to zero, pay-per-invocation, built-in partial batch failure handling.

### Option B: Fargate (Original)

\```
Adapty Webhooks
    │
    ▼
API Gateway (POST /ingest/{ct_account_id})
    │  ── validates Authorization header
    │  ── routes to per-account SQS queue
    ▼
SQS Queue (per account)
    │  ── standard queue, 14-day retention
    │  ── DLQ after 5 failed processing attempts
    ▼
Fargate Service (per account, Go)
    │  ── long-polls SQS
    │  ── transforms events
    │  ── uploads to CleverTap
    ▼
CleverTap Upload Events API
\```

**Advantages:** In-memory LRU dedup cache, health check endpoint, long-running process for consistent throughput.
```

- [ ] **Step 2: Add Lambda-specific deployment section**

Add a new section after the Fargate Task Definition section:

```markdown
## Lambda Deployment (Option A)

### Build & Deploy

Build the Lambda zip:
\```bash
make build-lambda
# produces bin/lambda.zip
\```

Upload `bin/lambda.zip` to Lambda directly or via S3.

### Lambda Configuration

| Setting | Value |
|---------|-------|
| Runtime | `provided.al2023` |
| Architecture | `arm64` |
| Handler | `bootstrap` (binary name) |
| Memory | 256 MB (adjust based on batch size) |
| Timeout | 60 seconds |

### SQS Event Source Mapping

| Setting | Value |
|---------|-------|
| Batch size | 10 (matches CleverTap API max) |
| Batch window | 30 seconds |
| Report batch item failures | **Enabled** (critical — allows partial batch retry) |
| Maximum concurrency | 5 (per account, prevents CleverTap rate limiting) |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CT_ACCOUNT_ID` | Yes | CleverTap account ID |
| `CT_PASSCODE` | Yes | CleverTap passcode (use Secrets Manager) |
| `CT_REGION` | Yes | CleverTap region: `eu1`, `us1`, `in1`, `sg1`, `mec1` |
| `LOG_LEVEL` | No | `debug`, `info` (default), `warn`, `error` |
| `TRANSFORM_CONFIG_PATH` | No | Path to transform config (bundle in zip or use Lambda layer) |

### IAM Role

\```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:{region}:{account-id}:adapty-ct-{account_id}"
    },
    {
      "Effect": "Allow",
      "Action": "logs:*",
      "Resource": "arn:aws:logs:{region}:{account-id}:log-group:/aws/lambda/adapty-ct-*"
    }
  ]
}
\```

Note: Lambda does **not** need dedup cache configuration — each invocation processes a single batch and SQS visibility timeout handles retry semantics.
```

- [ ] **Step 3: Update the deployment checklist**

Add Lambda items to the existing checklist:

```markdown
### Lambda Deployment Checklist
- [ ] Build Lambda zip: `make build-lambda`
- [ ] Create Lambda function with `provided.al2023` runtime, `arm64` architecture
- [ ] Upload `bin/lambda.zip` as function code
- [ ] Configure environment variables (CT_ACCOUNT_ID, CT_PASSCODE, CT_REGION)
- [ ] Store CT_PASSCODE in Secrets Manager and reference via Lambda env config
- [ ] Create IAM execution role with SQS + CloudWatch Logs permissions
- [ ] Create SQS event source mapping with batch size 10, window 30s, partial batch failures enabled
- [ ] Set maximum concurrency to 5 per account
- [ ] Test with a single webhook event and verify CleverTap receives it
- [ ] Set up CloudWatch alarms (DLQ, error rate, Lambda errors/throttles)
```

- [ ] **Step 4: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: add Lambda deployment option to architecture guide"
```

---

### Task 8: Run full verification

- [ ] **Step 1: Run the complete test suite**

Run: `cd /Users/dk/projects/adapty-ct-connector && go test ./... -v -count=1`
Expected: all tests pass across `processor`, `queue`, `transform`, `clevertap`, `backfill`

- [ ] **Step 2: Build all binaries**

Run: `cd /Users/dk/projects/adapty-ct-connector && make build`
Expected: `bin/connector`, `bin/lambda/bootstrap`, `bin/lambda.zip`, `bin/backfill` all created

- [ ] **Step 3: Verify Docker build still works**

Run: `cd /Users/dk/projects/adapty-ct-connector && docker build -t adapty-ct-connector:test .`
Expected: image builds successfully (Fargate path unbroken)

- [ ] **Step 4: Run go vet**

Run: `cd /Users/dk/projects/adapty-ct-connector && go vet ./...`
Expected: no issues
