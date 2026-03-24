#!/bin/bash
set -euo pipefail

awslocal sqs create-queue --queue-name adapty-ct-test
awslocal sqs create-queue --queue-name adapty-ct-test-dlq
echo "SQS queues created"
