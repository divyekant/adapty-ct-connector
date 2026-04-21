package processor

import (
	"encoding/json"
	"errors"
	"fmt"
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

// Process takes a batch of raw messages, transforms them, uploads to CleverTap,
// and returns per-message outcomes. The caller is responsible for SQS acknowledgment
// (deleting messages for OutcomeSuccess/OutcomeSkip, leaving OutcomeFail for retry).
func (p *Processor) Process(messages []InputMessage) *BatchResult {
	results := make([]MessageResult, len(messages))
	for i, m := range messages {
		results[i] = MessageResult{Index: i, MessageID: m.ID}
	}

	type emission struct {
		inputIndex int
		kind       string // RecordTypeProfile or RecordTypeEvent
		record     clevertap.EventRecord
		eventID    string
	}
	var emissions []emission
	emitted := make(map[int]bool)
	seenThisBatch := make(map[string]bool)

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

		if eventID != "" {
			if (p.dedup != nil && p.dedup.Contains(eventID)) || seenThisBatch[eventID] {
				slog.Debug("processor: duplicate event, skipping",
					"profile_event_id", eventID, "message_id", msg.ID)
				results[i].Outcome = OutcomeSkip
				continue
			}
		}

		eventRec, err := transform.Transform(evt, p.cfg)
		if err != nil {
			slog.Error("processor: transform error",
				"message_id", msg.ID, "event_type", evt.EventType, "err", err)
			results[i].Outcome = OutcomeFail
			results[i].Error = err
			continue
		}

		results[i].Identity = eventRec.Identity

		if profileRec, ok := transform.BuildProfileRecord(evt, p.cfg); ok {
			emissions = append(emissions, emission{
				inputIndex: i, kind: clevertap.RecordTypeProfile, record: profileRec, eventID: eventID,
			})
		}
		emissions = append(emissions, emission{
			inputIndex: i, kind: clevertap.RecordTypeEvent, record: eventRec, eventID: eventID,
		})
		emitted[i] = true
		if eventID != "" {
			seenThisBatch[eventID] = true
		}
	}

	if len(emissions) == 0 {
		return &BatchResult{Results: results}
	}

	records := make([]clevertap.EventRecord, len(emissions))
	for i, e := range emissions {
		records[i] = e.record
	}

	uploadStart := time.Now()
	resp, err := p.uploader.Upload(clevertap.UploadRequest{D: records})
	if err != nil {
		var authErr *clevertap.AuthError
		fatal := errors.As(err, &authErr)
		if fatal {
			slog.Error("processor: CleverTap authentication failure", "err", err)
		} else {
			slog.Error("processor: CleverTap upload error", "err", err)
		}
		for i := range messages {
			if emitted[i] {
				results[i].Outcome = OutcomeFail
				results[i].Error = err
			}
		}
		batch := &BatchResult{Results: results}
		if fatal {
			batch.FatalError = err
		}
		return batch
	}

	latencyMs := time.Since(uploadStart).Milliseconds()

	inputFailure := make(map[int]error)
	for _, u := range resp.Unprocessed {
		if u.Record < 0 || u.Record >= len(emissions) {
			continue
		}
		e := emissions[u.Record]
		slog.Warn("processor: record not processed by CleverTap",
			"kind", e.kind,
			"event_type", results[e.inputIndex].EventType,
			"identity", results[e.inputIndex].Identity,
			"profile_event_id", e.eventID,
			"status", u.Status,
			"code", u.Code,
			"error", u.Error,
			"latency_ms", latencyMs,
		)
		if _, exists := inputFailure[e.inputIndex]; !exists {
			inputFailure[e.inputIndex] = fmt.Errorf("clevertap: record rejected (code=%d status=%s): %s", u.Code, u.Status, u.Error)
		}
	}

	for i := range messages {
		if !emitted[i] {
			continue
		}
		if err, failed := inputFailure[i]; failed {
			results[i].Outcome = OutcomeFail
			results[i].Error = err
			continue
		}
		if results[i].EventID != "" && p.dedup != nil {
			p.dedup.Add(results[i].EventID, struct{}{})
		}
		slog.Info("processor: event processed",
			"event_type", results[i].EventType,
			"identity", results[i].Identity,
			"profile_event_id", results[i].EventID,
			"status", clevertap.StatusSuccess,
			"latency_ms", latencyMs,
		)
		results[i].Outcome = OutcomeSuccess
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
