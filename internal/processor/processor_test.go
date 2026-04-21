package processor

import (
	"encoding/json"
	"strings"
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
	for _, r := range result.Results {
		if r.Outcome != OutcomeFail {
			t.Errorf("expected OutcomeFail on auth error, got %d", r.Outcome)
		}
	}
}

// makeMessageWithEmail produces an Adapty event that triggers both a profile and an event record.
func makeMessageWithEmail(id, eventType, profileEventID, email string) InputMessage {
	evt := adapty.Event{
		ProfileID:     "profile-" + id,
		EventType:     eventType,
		EventDatetime: "2024-01-15T10:00:00.000000+0000",
		Email:         &email,
		EventProperties: map[string]interface{}{
			"profile_event_id": profileEventID,
		},
	}
	b, _ := json.Marshal(evt)
	return InputMessage{ID: "msg-" + id, Body: b}
}

func TestProcess_DualEmissionProfileFailureMapsToInput(t *testing.T) {
	// Single input emits two records (profile at emissions[0], event at emissions[1]).
	// CT fails the profile record — input must be marked OutcomeFail with an error that
	// contains the CT code/status/error message.
	ct := &mockUploader{
		response: &clevertap.UploadResponse{
			Status:    "partial",
			Processed: 1,
			Unprocessed: []clevertap.Unprocessed{
				{Status: "fail", Code: 42, Error: "bad profile", Record: 0},
			},
		},
	}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{
		makeMessageWithEmail("1", "subscription_started", "evt-001", "user@example.com"),
	}

	result := p.Process(msgs)
	if result.FatalError != nil {
		t.Fatalf("unexpected fatal error: %v", result.FatalError)
	}
	if len(ct.uploaded) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(ct.uploaded))
	}
	if len(ct.uploaded[0].D) != 2 {
		t.Fatalf("expected 2 records uploaded (profile + event), got %d", len(ct.uploaded[0].D))
	}
	if ct.uploaded[0].D[0].Type != clevertap.RecordTypeProfile {
		t.Errorf("expected first record type %q, got %q", clevertap.RecordTypeProfile, ct.uploaded[0].D[0].Type)
	}
	if ct.uploaded[0].D[1].Type != clevertap.RecordTypeEvent {
		t.Errorf("expected second record type %q, got %q", clevertap.RecordTypeEvent, ct.uploaded[0].D[1].Type)
	}
	if result.Results[0].Outcome != OutcomeFail {
		t.Errorf("expected OutcomeFail when profile record fails, got %d", result.Results[0].Outcome)
	}
	if result.Results[0].Error == nil {
		t.Fatal("expected non-nil error")
	}
	msg := result.Results[0].Error.Error()
	for _, want := range []string{"42", "fail", "bad profile"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message %q missing expected substring %q", msg, want)
		}
	}
}

func TestProcess_DualEmissionEventFailureMapsToInput(t *testing.T) {
	// Symmetric case: event record (emissions[1]) fails — the input must still be OutcomeFail.
	ct := &mockUploader{
		response: &clevertap.UploadResponse{
			Status:    "partial",
			Processed: 1,
			Unprocessed: []clevertap.Unprocessed{
				{Status: "fail", Code: 7, Error: "bad event", Record: 1},
			},
		},
	}
	cfg := transform.DefaultConfig()
	p := New(ct, cfg, 0)

	msgs := []InputMessage{
		makeMessageWithEmail("1", "subscription_started", "evt-001", "user@example.com"),
	}

	result := p.Process(msgs)
	if result.Results[0].Outcome != OutcomeFail {
		t.Errorf("expected OutcomeFail when event record fails, got %d", result.Results[0].Outcome)
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
