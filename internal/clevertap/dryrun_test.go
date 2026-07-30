package clevertap

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestDryRunUploadReturnsSuccessWithoutNetwork(t *testing.T) {
	d := NewDryRun()
	req := UploadRequest{D: []EventRecord{
		{Identity: "user-1", Type: RecordTypeEvent, EvtName: "subscription_started"},
		{Identity: "user-1", Type: RecordTypeProfile, ProfileData: map[string]interface{}{"Email": "a@b.c"}},
	}}

	resp, err := d.Upload(req)
	if err != nil {
		t.Fatalf("dry-run upload must never fail, got %v", err)
	}
	if resp.Status != StatusSuccess {
		t.Fatalf("expected status %q, got %q", StatusSuccess, resp.Status)
	}
	if resp.Processed != 2 {
		t.Fatalf("expected 2 processed, got %d", resp.Processed)
	}
	if len(resp.Unprocessed) != 0 {
		t.Fatalf("expected no unprocessed, got %v", resp.Unprocessed)
	}
}

func TestDryRunUploadLogsRecords(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	d := NewDryRun()
	_, err := d.Upload(UploadRequest{D: []EventRecord{
		{Identity: "user-42", Type: RecordTypeEvent, EvtName: "trial_converted"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"dry_run", "user-42", "trial_converted"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got: %s", want, out)
		}
	}
}
