package main

import (
	"os"
	"testing"
)

func TestReadNDJSON(t *testing.T) {
	content := `{"profile_id":"p1","event_type":"subscription_started","event_datetime":"2024-01-01T00:00:00.000000+0000"}
{"profile_id":"p2","event_type":"trial_started","event_datetime":"2024-01-02T00:00:00.000000+0000"}
`
	f, err := os.CreateTemp(t.TempDir(), "test-*.ndjson")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	f.Close()

	events, err := readNDJSON(f.Name(), 0)
	if err != nil {
		t.Fatalf("readNDJSON: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].EventType != "subscription_started" {
		t.Errorf("event[0].EventType = %q, want %q", events[0].EventType, "subscription_started")
	}
	if events[1].EventType != "trial_started" {
		t.Errorf("event[1].EventType = %q, want %q", events[1].EventType, "trial_started")
	}
}

func TestReadNDJSON_WithOffset(t *testing.T) {
	content := `{"profile_id":"p1","event_type":"subscription_started","event_datetime":"2024-01-01T00:00:00.000000+0000"}
{"profile_id":"p2","event_type":"trial_started","event_datetime":"2024-01-02T00:00:00.000000+0000"}
{"profile_id":"p3","event_type":"subscription_expired","event_datetime":"2024-01-03T00:00:00.000000+0000"}
`
	f, err := os.CreateTemp(t.TempDir(), "test-*.ndjson")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	f.Close()

	events, err := readNDJSON(f.Name(), 2)
	if err != nil {
		t.Fatalf("readNDJSON: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventType != "subscription_expired" {
		t.Errorf("event[0].EventType = %q, want %q", events[0].EventType, "subscription_expired")
	}
}
