package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("POST /1/upload", handleUpload)

	slog.Info("mock CleverTap server starting", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	accountID := r.Header.Get("X-CleverTap-Account-Id")
	passcode := r.Header.Get("X-CleverTap-Passcode")
	if accountID == "" || passcode == "" {
		slog.Warn("missing auth headers",
			"has_account_id", accountID != "",
			"has_passcode", passcode != "",
		)
		http.Error(w, `{"status":"error","message":"missing auth headers"}`, http.StatusUnauthorized)
		return
	}

	ct := r.Header.Get("Content-Type")
	if ct != "application/json" {
		slog.Warn("unexpected Content-Type", "content_type", ct)
	}

	var req clevertap.UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("failed to decode request body", "error", err)
		http.Error(w, `{"status":"error","message":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	processed := 0
	for i, rec := range req.D {
		var missing []string
		if rec.Identity == "" {
			missing = append(missing, "identity")
		}
		if rec.EvtName == "" {
			missing = append(missing, "evtName")
		}
		if rec.Type == "" {
			missing = append(missing, "type")
		}
		if rec.TS == 0 {
			missing = append(missing, "ts")
		}
		if len(missing) > 0 {
			slog.Error("record missing required fields",
				"record_index", i,
				"missing_fields", missing,
			)
			continue
		}

		slog.Info("received event",
			"record_index", i,
			"identity", rec.Identity,
			"evtName", rec.EvtName,
			"ts", rec.TS,
			"property_count", len(rec.EvtData),
		)
		processed++
	}

	resp := clevertap.UploadResponse{
		Status:    "success",
		Processed: processed,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}
