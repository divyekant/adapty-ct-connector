package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

func main() {
	ctAccountID := flag.String("ct-account-id", "", "CleverTap account ID (required)")
	ctPasscode := flag.String("ct-passcode", "", "CleverTap passcode (required)")
	ctRegion := flag.String("ct-region", "", "CleverTap region (required)")
	input := flag.String("input", "", "Path to NDJSON input file (required)")
	batchSize := flag.Int("batch-size", 500, "Number of events per batch (max 1000)")
	concurrency := flag.Int("concurrency", 5, "Number of concurrent upload workers (max 15)")
	offset := flag.Int("offset", 0, "Skip first N records")
	dryRun := flag.Bool("dry-run", false, "Log batches without uploading")
	transformConfig := flag.String("transform-config", "", "Path to transform config JSON (optional)")
	flag.Parse()

	// Validate required flags.
	var missing []string
	if *ctAccountID == "" {
		missing = append(missing, "--ct-account-id")
	}
	if *ctPasscode == "" {
		missing = append(missing, "--ct-passcode")
	}
	if *ctRegion == "" {
		missing = append(missing, "--ct-region")
	}
	if *input == "" {
		missing = append(missing, "--input")
	}
	if len(missing) > 0 {
		for _, m := range missing {
			slog.Error("missing required flag", "flag", m)
		}
		flag.Usage()
		os.Exit(1)
	}

	// Cap values.
	if *batchSize > 1000 {
		*batchSize = 1000
	}
	if *concurrency > 15 {
		*concurrency = 15
	}

	// Load transform config.
	cfg, err := transform.LoadConfig(*transformConfig)
	if err != nil {
		slog.Error("failed to load transform config", "err", err)
		os.Exit(1)
	}

	events, err := readNDJSON(*input, *offset)
	if err != nil {
		slog.Error("failed to read input file", "err", err)
		os.Exit(1)
	}

	slog.Info("backfill starting",
		"file", *input, "total_events", len(events),
		"batch_size", *batchSize, "concurrency", *concurrency,
		"offset", *offset, "dry_run", *dryRun)

	// Create CleverTap client (unless dry-run).
	var client *clevertap.Client
	if !*dryRun {
		client = clevertap.NewClientFromRegion(*ctAccountID, *ctPasscode, *ctRegion)
	}

	// Split into batches and process with semaphore-limited concurrency.
	var processed, failed atomic.Int64
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(events); i += *batchSize {
		end := i + *batchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]
		batchNum := i / *batchSize

		sem <- struct{}{}
		wg.Add(1)
		go func(batchNum int, batch []adapty.Event) {
			defer wg.Done()
			defer func() { <-sem }()

			records, errs := transform.TransformBatch(batch, cfg)
			if len(errs) > 0 {
				failed.Add(int64(len(errs)))
				for _, e := range errs {
					slog.Error("transform error", "batch", batchNum, "err", e)
				}
			}

			if len(records) == 0 {
				slog.Warn("no records after transform", "batch", batchNum)
				return
			}

			if *dryRun {
				slog.Info("dry-run batch", "batch", batchNum, "records", len(records))
				processed.Add(int64(len(records)))
				return
			}

			resp, err := client.Upload(clevertap.UploadRequest{D: records})
			if err != nil {
				failed.Add(int64(len(records)))
				slog.Error("upload failed", "batch", batchNum, "err", err)
				return
			}

			processed.Add(int64(resp.Processed))
			failed.Add(int64(len(resp.Unprocessed)))
			slog.Info("batch complete",
				"batch", batchNum, "status", resp.Status,
				"processed", resp.Processed, "unprocessed", len(resp.Unprocessed))
		}(batchNum, batch)
	}

	wg.Wait()

	slog.Info("backfill complete", "processed", processed.Load(), "failed", failed.Load())
}

// readNDJSON opens a file and parses it line by line as NDJSON.
// It skips the first offset lines and empty lines.
func readNDJSON(path string, offset int) ([]adapty.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024) // 1MB buffer
	scanner.Buffer(buf, len(buf))

	var events []adapty.Event
	lineNum := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if lineNum < offset {
			lineNum++
			continue
		}
		lineNum++

		var event adapty.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("parse line %d: %w", lineNum, err)
		}
		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}

	return events, nil
}
