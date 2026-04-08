package queue

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSAdapter struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSAdapter(client *sqs.Client, queueURL string) *SQSAdapter {
	return &SQSAdapter{
		client:   client,
		queueURL: queueURL,
	}
}

func (a *SQSAdapter) ReceiveMessages(ctx context.Context, maxMessages int) ([]Message, error) {
	if maxMessages > sqsMaxMessages {
		maxMessages = sqsMaxMessages
	}

	out, err := a.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.queueURL),
		MaxNumberOfMessages: int32(maxMessages),
		WaitTimeSeconds:     20,
	})
	if err != nil {
		return nil, fmt.Errorf("sqs: receive message: %w", err)
	}

	msgs := make([]Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msg := Message{}
		if m.Body != nil {
			msg.Body = *m.Body
		}
		if m.ReceiptHandle != nil {
			msg.ReceiptHandle = *m.ReceiptHandle
		}
		if m.MessageId != nil {
			msg.MessageID = *m.MessageId
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (a *SQSAdapter) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := a.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(a.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs: delete message: %w", err)
	}
	return nil
}
