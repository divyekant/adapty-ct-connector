package queue

import (
	"context"
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

// CTUploader abstracts the CleverTap upload operation.
type CTUploader interface {
	Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error)
}

const sqsMaxMessages = 10

type Consumer struct {
	sqs       SQSClient
	ct        CTUploader
	cfg       *transform.Config
	batchSize int
	dedup     *lru.Cache[string, struct{}]
}

// NewConsumer creates a Consumer. batchSize is capped at 10 (SQS limit); dedupSize controls
// the LRU capacity for deduplication.
func NewConsumer(sqs SQSClient, ct CTUploader, cfg *transform.Config, batchSize, dedupSize int) *Consumer {
	if batchSize > sqsMaxMessages {
		batchSize = sqsMaxMessages
	}
	cache, err := lru.New[string, struct{}](dedupSize)
	if err != nil {
		// Only fails if dedupSize <= 0; treat as a programming error.
		panic(fmt.Sprintf("queue: invalid dedupSize %d: %v", dedupSize, err))
	}
	return &Consumer{
		sqs:       sqs,
		ct:        ct,
		cfg:       cfg,
		batchSize: batchSize,
		dedup:     cache,
	}
}

// Run starts the polling loop. It calls ProcessBatch in a tight loop, calls onLoop after each
// iteration (useful for tests / metrics), and sleeps 1 second when the queue is empty.
// The loop exits when ctx is cancelled.
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

// ProcessBatch receives up to batchSize messages from SQS, transforms and uploads them to
// CleverTap, and deletes successfully processed messages. It returns the count of records
// successfully processed by CleverTap.
func (c *Consumer) ProcessBatch(ctx context.Context) (int, error) {
	msgs, err := c.sqs.ReceiveMessages(ctx, c.batchSize)
	if err != nil {
		return 0, fmt.Errorf("queue: receive messages: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	type item struct {
		record    clevertap.EventRecord
		msg       Message
		eventID   string
		eventType string
	}

	var items []item

	for _, msg := range msgs {
		var evt adapty.Event
		if err := json.Unmarshal([]byte(msg.Body), &evt); err != nil {
			slog.Error("queue: malformed message, leaving for redelivery",
				"message_id", msg.MessageID, "err", err)
			continue
		}

		if evt.EventType == "" {
			slog.Debug("queue: empty event_type (verification request), deleting",
				"message_id", msg.MessageID)
			if delErr := c.sqs.DeleteMessage(ctx, msg.ReceiptHandle); delErr != nil {
				slog.Error("queue: delete verification message", "err", delErr)
			}
			continue
		}

		eventID := getEventID(evt)
		if eventID != "" {
			if c.dedup.Contains(eventID) {
				slog.Debug("queue: duplicate event, deleting",
					"profile_event_id", eventID, "message_id", msg.MessageID)
				if delErr := c.sqs.DeleteMessage(ctx, msg.ReceiptHandle); delErr != nil {
					slog.Error("queue: delete duplicate message", "err", delErr)
				}
				continue
			}
		}

		record, err := transform.Transform(evt, c.cfg)
		if err != nil {
			slog.Error("queue: transform error, leaving for redelivery",
				"message_id", msg.MessageID, "event_type", evt.EventType, "err", err)
			continue
		}

		if eventID != "" {
			c.dedup.Add(eventID, struct{}{})
		}

		items = append(items, item{
			record:    record,
			msg:       msg,
			eventID:   eventID,
			eventType: evt.EventType,
		})
	}

	if len(items) == 0 {
		return 0, nil
	}

	records := make([]clevertap.EventRecord, len(items))
	for i, it := range items {
		records[i] = it.record
	}

	uploadStart := time.Now()
	resp, err := c.ct.Upload(clevertap.UploadRequest{D: records})
	if err != nil {
		var authErr *clevertap.AuthError
		if errors.As(err, &authErr) {
			slog.Error("queue: CleverTap authentication failure — check credentials",
				"err", err)
			return 0, err
		}
		slog.Error("queue: CleverTap upload error, messages stay in SQS", "err", err)
		return 0, err
	}

	// Build set of failed record indices from the response.
	failedIdx := make(map[int]clevertap.Unprocessed, len(resp.Unprocessed))
	for _, u := range resp.Unprocessed {
		failedIdx[u.Record] = u
	}

	latencyMs := time.Since(uploadStart).Milliseconds()
	successCount := 0
	for i, it := range items {
		if u, failed := failedIdx[i]; failed {
			slog.Warn("queue: event not processed by CleverTap",
				"event_type", it.eventType,
				"identity", it.record.Identity,
				"profile_event_id", it.eventID,
				"status", u.Status,
				"code", u.Code,
				"error", u.Error,
				"latency_ms", latencyMs,
			)
			continue
		}

		slog.Info("queue: event processed",
			"event_type", it.eventType,
			"identity", it.record.Identity,
			"profile_event_id", it.eventID,
			"status", clevertap.StatusSuccess,
			"latency_ms", latencyMs,
		)

		if delErr := c.sqs.DeleteMessage(ctx, it.msg.ReceiptHandle); delErr != nil {
			slog.Error("queue: delete processed message", "message_id", it.msg.MessageID, "err", delErr)
		}
		successCount++
	}

	return successCount, nil
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
