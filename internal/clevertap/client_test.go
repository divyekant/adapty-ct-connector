package clevertap

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// makeRequest is a helper that builds a minimal UploadRequest with one event.
func makeRequest() UploadRequest {
	return UploadRequest{
		D: []EventRecord{
			{
				Identity: "user-1",
				TS:       1700000000,
				Type:     "event",
				EvtName:  "TestEvent",
				EvtData:  map[string]interface{}{"key": "value"},
			},
		},
	}
}

func TestClient_Upload_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify required headers.
		if got := r.Header.Get("X-CleverTap-Account-Id"); got != "acc-123" {
			t.Errorf("X-CleverTap-Account-Id = %q, want %q", got, "acc-123")
		}
		if got := r.Header.Get("X-CleverTap-Passcode"); got != "pass-abc" {
			t.Errorf("X-CleverTap-Passcode = %q, want %q", got, "pass-abc")
		}
		if got := r.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want %q", got, "application/json; charset=utf-8")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(UploadResponse{
			Status:    "success",
			Processed: 1,
		})
	}))
	defer srv.Close()

	client := NewClient("acc-123", "pass-abc", srv.URL)
	resp, err := client.Upload(makeRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "success" {
		t.Errorf("status = %q, want %q", resp.Status, "success")
	}
	if resp.Processed != 1 {
		t.Errorf("processed = %d, want 1", resp.Processed)
	}
}

func TestClient_Upload_Partial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(UploadResponse{
			Status:    "partial",
			Processed: 1,
			Unprocessed: []Unprocessed{
				{Status: "fail", Code: 512, Error: "invalid record", Record: 1},
			},
		})
	}))
	defer srv.Close()

	client := NewClient("acc-id", "pass", srv.URL)
	resp, err := client.Upload(makeRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "partial" {
		t.Errorf("status = %q, want %q", resp.Status, "partial")
	}
	if len(resp.Unprocessed) != 1 {
		t.Errorf("unprocessed count = %d, want 1", len(resp.Unprocessed))
	}
}

func TestClient_Upload_Retries5xx(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(UploadResponse{
			Status:    "success",
			Processed: 1,
		})
	}))
	defer srv.Close()

	client := NewClient("acc-id", "pass", srv.URL)
	client.InitialBackoff = 0 // no sleep in tests

	resp, err := client.Upload(makeRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "success" {
		t.Errorf("status = %q, want %q", resp.Status, "success")
	}
	if callCount != 3 {
		t.Errorf("callCount = %d, want 3", callCount)
	}
}

func TestClient_Upload_AuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := NewClient("bad-acc", "bad-pass", srv.URL)
	_, err := client.Upload(makeRequest())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if authErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("StatusCode = %d, want %d", authErr.StatusCode, http.StatusUnauthorized)
	}
}

func TestClient_Upload_BadRequest(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	client := NewClient("acc-id", "pass", srv.URL)
	_, err := client.Upload(makeRequest())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Must not retry — only 1 call should happen.
	if callCount != 1 {
		t.Errorf("callCount = %d, want 1 (no retries on 400)", callCount)
	}
}

func TestNewClientFromRegion(t *testing.T) {
	client := NewClientFromRegion("acc-id", "pass", "eu1")
	want := "https://eu1.api.clevertap.com/1/upload"
	if client.baseURL != want {
		t.Errorf("baseURL = %q, want %q", client.baseURL, want)
	}
	if client.accountID != "acc-id" {
		t.Errorf("accountID = %q, want %q", client.accountID, "acc-id")
	}
	if client.passcode != "pass" {
		t.Errorf("passcode = %q, want %q", client.passcode, "pass")
	}
	if client.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", client.MaxRetries)
	}
	if client.InitialBackoff != time.Second {
		t.Errorf("InitialBackoff = %v, want 1s", client.InitialBackoff)
	}
}
