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
