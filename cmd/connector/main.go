package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/processor"
	"github.com/anthropic/adapty-ct-connector/internal/queue"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

func main() {
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
	sqsQueueURL := requireEnv("SQS_QUEUE_URL")

	batchSize := envInt("BATCH_SIZE", 10)
	dedupLRUSize := envInt("DEDUP_LRU_SIZE", 100000)
	dryRun := os.Getenv("DRY_RUN") == "true"
	transformConfigPath := os.Getenv("TRANSFORM_CONFIG_PATH")
	ctBaseURL := os.Getenv("CT_BASE_URL")
	sqsEndpoint := os.Getenv("SQS_ENDPOINT")

	cfg, err := transform.LoadConfig(transformConfigPath)
	if err != nil {
		slog.Error("failed to load transform config", "path", transformConfigPath, "err", err)
		os.Exit(1)
	}

	var uploader processor.Uploader
	if dryRun {
		uploader = &dryRunUploader{}
		slog.Info("dry-run mode enabled: events will be logged but not sent to CleverTap")
	} else if ctBaseURL != "" {
		uploader = clevertap.NewClient(accountID, passcode, ctBaseURL)
		slog.Info("using custom CT_BASE_URL", "base_url", ctBaseURL)
	} else {
		uploader = clevertap.NewClientFromRegion(accountID, passcode, ctRegion)
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("failed to load AWS config", "err", err)
		os.Exit(1)
	}

	var sqsOpts []func(*sqs.Options)
	if sqsEndpoint != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(sqsEndpoint)
		})
		slog.Info("using custom SQS_ENDPOINT", "endpoint", sqsEndpoint)
	}

	sqsClient := sqs.NewFromConfig(awsCfg, sqsOpts...)
	adapter := queue.NewSQSAdapter(sqsClient, sqsQueueURL)

	consumer := queue.NewConsumer(adapter, uploader, cfg, batchSize, dedupLRUSize)

	var lastSuccess atomic.Int64
	lastSuccess.Store(time.Now().Unix()) // initialise so health check passes on startup

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		last := lastSuccess.Load()
		age := time.Now().Unix() - last
		if age < 60 {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "ok")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "stale: last poll %ds ago", age)
		}
	})

	go func() {
		srv := &http.Server{Addr: ":8080", Handler: mux}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health check server error", "err", err)
		}
	}()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	slog.Info("connector starting",
		"batch_size", batchSize,
		"dedup_lru_size", dedupLRUSize,
		"dry_run", dryRun,
		"ct_region", ctRegion,
		"sqs_queue_url", sqsQueueURL,
		"log_level", os.Getenv("LOG_LEVEL"),
		"transform_config", transformConfigPath,
	)

	consumer.Run(runCtx, func() {
		lastSuccess.Store(time.Now().Unix())
	})

	slog.Info("connector stopped")
}

// requireEnv returns the value of an env var or exits with a message if it is unset.
func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

// envInt returns the integer value of an env var, or the default if unset or invalid.
func envInt(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		slog.Warn("invalid integer env var, using default", "key", key, "value", s, "default", def)
		return def
	}
	return n
}

// dryRunUploader implements CTUploader by logging and returning success.
type dryRunUploader struct{}

func (d *dryRunUploader) Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error) {
	slog.Info("dry-run: would upload events", "count", len(req.D))
	return &clevertap.UploadResponse{Status: clevertap.StatusSuccess}, nil
}
