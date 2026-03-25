package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/processor"
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

const sqsMaxMessages = 10

type Consumer struct {
	sqs       SQSClient
	proc      *processor.Processor
	batchSize int
}

// NewConsumer creates a Consumer that polls SQS and delegates processing to the shared Processor.
func NewConsumer(sqs SQSClient, uploader processor.Uploader, cfg *transform.Config, batchSize, dedupSize int) *Consumer {
	if batchSize > sqsMaxMessages {
		batchSize = sqsMaxMessages
	}
	return &Consumer{
		sqs:       sqs,
		proc:      processor.New(uploader, cfg, dedupSize),
		batchSize: batchSize,
	}
}

// Run starts the polling loop. It calls ProcessBatch in a tight loop, calls onLoop after each
// iteration, and sleeps 1 second when the queue is empty.
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

// ProcessBatch receives messages from SQS, delegates to the processor, and
// deletes messages that were successfully processed or skipped.
func (c *Consumer) ProcessBatch(ctx context.Context) (int, error) {
	msgs, err := c.sqs.ReceiveMessages(ctx, c.batchSize)
	if err != nil {
		return 0, fmt.Errorf("queue: receive messages: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	// Convert to processor input
	input := make([]processor.InputMessage, len(msgs))
	for i, m := range msgs {
		input[i] = processor.InputMessage{
			ID:   m.MessageID,
			Body: []byte(m.Body),
		}
	}

	result := c.proc.Process(input)

	if result.FatalError != nil {
		return 0, result.FatalError
	}

	successCount := 0
	for i, r := range result.Results {
		switch r.Outcome {
		case processor.OutcomeSuccess, processor.OutcomeSkip:
			if delErr := c.sqs.DeleteMessage(ctx, msgs[i].ReceiptHandle); delErr != nil {
				slog.Error("queue: delete message", "message_id", msgs[i].MessageID, "err", delErr)
				continue
			}
			if r.Outcome == processor.OutcomeSuccess {
				successCount++
			}
		case processor.OutcomeFail:
			// Leave in SQS for retry
		}
	}

	return successCount, nil
}
