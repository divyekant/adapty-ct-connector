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
		suffix := b
		if len(suffix) > 8 {
			suffix = suffix[:8]
		}
		msgs[i] = Message{
			Body:          b,
			ReceiptHandle: "rh-" + suffix,
			MessageID:     "id-" + suffix,
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
