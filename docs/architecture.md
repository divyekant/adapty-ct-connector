# Adapty-to-CleverTap Connector — Infrastructure Architecture

This document describes the AWS infrastructure required to deploy the Adapty-to-CleverTap connector. Follow this guide to set up API Gateway, SQS, Fargate, and CloudWatch monitoring.

---

## Architecture Overview

```
Adapty Webhooks
    │
    ▼
API Gateway (POST /ingest/{ct_account_id})
    │  ── validates Authorization header
    │  ── routes to per-account SQS queue
    ▼
SQS Queue (per account)
    │  ── standard queue, 14-day retention
    │  ── DLQ after 5 failed processing attempts
    ▼
Fargate Service (per account, Go)
    │  ── long-polls SQS
    │  ── transforms events
    │  ── uploads to CleverTap
    ▼
CleverTap Upload Events API
```

**Data Flow:**
1. Adapty sends webhook events to API Gateway
2. API Gateway validates the Authorization header against SSM Parameter Store
3. API Gateway routes the event directly to the per-account SQS queue
4. Fargate task continuously polls the queue and processes events
5. Connector transforms and batches events, then uploads to CleverTap
6. Failed events after 5 retries move to DLQ for manual review

---

## API Gateway Setup

### REST API Configuration

**Resource:** `/ingest/{ct_account_id}`
- **Method:** POST
- **Path parameter:** `ct_account_id` — the CleverTap account identifier (alphanumeric)

### Authorization Header Validation

Adapty sends a configurable Authorization header with each webhook request. The expected header value must be stored in **AWS Systems Manager Parameter Store** at:
```
/adapty-ct-connector/{environment}/authorization-key
```

**Configuration Options:**

**Option A: API Key (Recommended for simplicity)**
- Create an API Key in API Gateway
- Attach an API key policy to the REST API
- Adapty provides the key value; store it in the Parameter Store path above
- API Gateway validates the key on each request

**Option B: Lambda Authorizer (Recommended for flexibility)**
- Create a Lambda function that:
  - Retrieves the expected Authorization header from Parameter Store (with caching)
  - Compares the incoming Authorization header value
  - Returns an Allow/Deny policy
- Attach the Lambda authorizer to the `/ingest/{ct_account_id}` POST method
- Supports custom header names and validation logic

### SQS Service Integration (No Lambda)

To minimize latency and cost, use a **direct AWS Service integration** from API Gateway to SQS:

1. **Create integration:**
   - Integration type: AWS Service
   - Service: SQS
   - HTTP method: POST
   - Action: SendMessage

2. **Path parameter mapping:**
   - Map the `{ct_account_id}` path parameter to the SQS queue name
   - Queue name format: `adapty-ct-{ct_account_id}`
   - Example: if `ct_account_id=abc123`, route to queue `adapty-ct-abc123`

3. **Request mapping template (application/json):**
   ```
   Action=SendMessage&MessageBody=$input.json('$')&QueueUrl=https://sqs.{region}.amazonaws.com/{account-id}/adapty-ct-{ct_account_id}
   ```

4. **Response mapping:**
   - Status code 200 for successful enqueue
   - Body: `{"status": "enqueued"}`

### Verification Payload Handling

Adapty sends a verification request (empty JSON `{}` or empty body) when the webhook is first configured.

**Return 200 immediately** — API Gateway's SQS integration will automatically send a SendMessage action. If the message body is empty, Fargate will skip processing and log accordingly.

**Webhook latency requirement:** Adapty expects a response within 10 seconds. The API Gateway → SQS direct integration is near-instant (< 100ms), so this is satisfied.

---

## Per-Account Resource Provisioning

For each CleverTap account, provision the following AWS resources:

### SQS Queue

**Queue name:** `adapty-ct-{account_id}`

**Configuration:**
- **Queue type:** Standard (not FIFO)
- **Message retention period:** 14 days (1,209,600 seconds)
- **Visibility timeout:** 300 seconds (5 minutes) — must be longer than max Fargate task processing time
- **Receive message wait time:** 20 seconds (long polling in Fargate)
- **Dead Letter Queue (DLQ):** Enable with redrive policy (see below)

### Dead Letter Queue (DLQ)

**DLQ queue name:** `adapty-ct-{account_id}-dlq`

**Configuration:**
- **Queue type:** Standard
- **Message retention period:** 14 days
- **Redrive policy on main queue:**
  ```json
  {
    "deadLetterTargetArn": "arn:aws:sqs:{region}:{account-id}:adapty-ct-{account_id}-dlq",
    "maxReceiveCount": 5
  }
  ```

Messages that fail processing 5 times (due to visibility timeout expiry) automatically move to the DLQ for manual inspection and replay.

### IAM Role for Fargate Task

**Role name:** `adapty-ct-connector-task-role-{account_id}`

**Permissions needed:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": [
        "arn:aws:sqs:{region}:{account-id}:adapty-ct-{account_id}",
        "arn:aws:sqs:{region}:{account-id}:adapty-ct-{account_id}-dlq"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:{region}:{account-id}:parameter/adapty-ct-connector/{environment}/*"
    }
  ]
}
```

---

## Fargate Task Definition

### Basic Configuration

**Container image:** `{account-id}.dkr.ecr.{region}.amazonaws.com/adapty-ct-connector:latest`
(Update the account ID, region, and tag to match your ECR repository)

**CPU:** 256 (0.25 vCPU)
**Memory:** 512 MB
**Desired count:** 1 per account

### Health Check

**HTTP endpoint:** `GET http://localhost:8080/healthz`
- **Interval:** 30 seconds
- **Timeout:** 5 seconds
- **Healthy threshold:** 2 consecutive successful checks
- **Unhealthy threshold:** 3 consecutive failed checks

The connector exposes this endpoint for ECS to monitor task health. If health checks fail consistently, ECS will restart the task.

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CT_ACCOUNT_ID` | Yes | — | CleverTap account ID |
| `CT_PASSCODE` | Yes | — | CleverTap account passcode (store in Secrets Manager, not as plain env var in production) |
| `CT_REGION` | Yes | — | CleverTap region: `eu1`, `us1`, `in1`, `sg1`, or `mec1` |
| `SQS_QUEUE_URL` | Yes | — | Full URL of the SQS queue, e.g., `https://sqs.us-east-1.amazonaws.com/123456789/adapty-ct-{account_id}` |
| `LOG_LEVEL` | No | `info` | Verbosity: `debug`, `info`, `warn`, or `error` |
| `DRY_RUN` | No | `false` | If `true`, log events but do not upload to CleverTap |
| `TRANSFORM_CONFIG_PATH` | No | — | Path to field exclusion/transform config file (e.g., `/etc/config/transform.json`) |
| `BATCH_SIZE` | No | `10` | Events per CleverTap API call (max: 10, recommended: 10) |
| `DEDUP_LRU_SIZE` | No | `100000` | Size of in-memory deduplication cache (events per task) |

### Secrets Management

**Do not store secrets as plain environment variables.** Instead:

1. Create an AWS Secrets Manager secret:
   ```
   /adapty-ct-connector/{environment}/{account_id}/credentials
   ```
   with JSON:
   ```json
   {
     "ct_passcode": "your-passcode-here"
   }
   ```

2. Update task definition to use `valueFrom` for the `CT_PASSCODE`:
   ```json
   {
     "name": "CT_PASSCODE",
     "valueFrom": "arn:aws:secretsmanager:{region}:{account-id}:secret:/adapty-ct-connector/{environment}/{account_id}/credentials:ct_passcode::"
   }
   ```

3. Update the task execution role to include:
   ```json
   {
     "Effect": "Allow",
     "Action": "secretsmanager:GetSecretValue",
     "Resource": "arn:aws:secretsmanager:{region}:{account-id}:secret:/adapty-ct-connector/*"
   }
   ```

### Container Port Configuration

- **Expose port:** 8080
- **Protocol:** TCP
- Used for health checks and optional metrics endpoints

---

## CloudWatch Monitoring & Alarms

### 1. DLQ Message Visibility

**Metric:** `ApproximateNumberOfMessagesVisible` on the DLQ queue

**Alarm Configuration:**
- Namespace: `AWS/SQS`
- Metric name: `ApproximateNumberOfMessagesVisible`
- Dimensions: `QueueName` = `adapty-ct-{account_id}-dlq`
- Threshold: > 0 messages
- Evaluation period: 1 minute
- Action: Send SNS notification to ops team

**Interpretation:** DLQ messages indicate events failed to process after 5 retries. Investigate the Fargate logs and replay if corrected.

### 2. Consumer Error Rate

**Log filter metric:**

1. In CloudWatch Logs, create a metric filter on the Fargate task log group:
   - Log group name: `/ecs/adapty-ct-connector/{account_id}`
   - Filter pattern: `[timestamp, request_id, level="ERROR", ...]` (adjust for your log format)
   - Metric name: `ErrorCount`
   - Metric namespace: `AdaptyCTConnector/{account_id}`
   - Metric value: 1

2. Create alarm:
   - Metric: `ErrorCount`
   - Threshold: > 10 errors in 5 minutes
   - Action: SNS notification

### 3. Task Health Check Failures

**Metric:** ECS task health check status

**Alarm Configuration:**
- CloudWatch metric: `ECS/HealthCheckStatus` (if available via container insights)
- Or: Monitor task restart count via ECS API
- Threshold: Any unhealthy check count > 0 for > 2 consecutive checks
- Action: SNS notification + auto-restart (handled by ECS desired count)

### 4. SQS Queue Depth

**Metric:** `ApproximateNumberOfMessagesVisible` on the main queue

**Monitor for baseline:** After 24 hours of normal operation, establish a baseline. Alert if queue depth consistently grows (indicates processing slower than ingestion).

---

## Adapty Webhook Configuration

### Dashboard Setup

1. Log in to Adapty dashboard
2. Navigate to **Integrations → Webhooks**
3. Create a new webhook with these settings:

**Endpoint URL:**
```
https://{api-gateway-domain}/ingest/{ct_account_id}
```

**Example:**
```
https://abc123.execute-api.us-east-1.amazonaws.com/prod/ingest/abc123
```

**Authorization Header:**
- Header name: `Authorization` (or custom, if using Lambda authorizer)
- Value: Retrieve from SSM Parameter Store at `/adapty-ct-connector/{environment}/authorization-key`
- Example: `Bearer your-secret-key-here`

**Event Types:** Enable all relevant event types:
- Subscription events
- Trial events
- Purchase events
- Refund events
- Cancellation events
- Etc.

**Attributes to send:**
- ✓ Send Attribution
- ✓ Send User Attributes
- ✓ Send Play Store purchase token

**Test & Verification:**
After saving, Adapty sends a verification request (empty JSON). The API should return HTTP 200 within 10 seconds. Confirm in the dashboard that the webhook is "active" before production use.

---

## Deployment Checklist

- [ ] Create API Gateway REST API with `/ingest/{ct_account_id}` resource
- [ ] Configure authorization (API key or Lambda authorizer)
- [ ] Set up API Gateway → SQS direct integration
- [ ] Create SQS queue `adapty-ct-{account_id}` with 14-day retention
- [ ] Create DLQ `adapty-ct-{account_id}-dlq` with redrive policy (maxReceiveCount: 5)
- [ ] Push Docker image to ECR (tag: `latest`)
- [ ] Create task execution role and task role with appropriate permissions
- [ ] Create Fargate task definition with environment variables and secrets
- [ ] Create ECS service with desired count = 1, enable auto-restart
- [ ] Configure health check endpoint on port 8080
- [ ] Create CloudWatch log group: `/ecs/adapty-ct-connector/{account_id}`
- [ ] Set up CloudWatch alarms (DLQ, error rate, health check)
- [ ] Store authorization key in SSM Parameter Store
- [ ] Configure Adapty webhook endpoint and test verification request
- [ ] Monitor logs and queue metrics for first 24 hours

---

## Scaling Notes

### Baseline Capacity

One Fargate task (256 CPU, 512 MB memory) handles approximately **50 million events per month** with default settings (`BATCH_SIZE=10`, `DEDUP_LRU_SIZE=100000`).

**For accounts > 100M events/month:**
- Increase `BATCH_SIZE` to 10 (max supported) — reduces API calls to CleverTap
- Add a second Fargate task to the same account by increasing the ECS service desired count to 2
- Tasks will auto-balance message processing via SQS

### Fargate Scaling Strategy

- **CPU bottleneck:** Increase task CPU to 512 (0.5 vCPU) and memory to 1024 MB
- **Memory bottleneck:** Increase `DEDUP_LRU_SIZE` if needed, or reduce memory per task and scale horizontally (multiple tasks)
- **Queue depth growth:** If ApproximateNumberOfMessagesVisible grows over time, add another task

### SQS Limits

- Standard SQS has **unlimited throughput** — no per-queue rate limits
- Message size limit: 256 KB (Adapty events are typically < 10 KB)
- Retention: 14 days (configurable up to 14 days max)

---

## Troubleshooting

### Webhook Requests Return 403

- **Cause:** Authorization header mismatch
- **Fix:** Verify the Authorization header value in Adapty matches the value in SSM Parameter Store

### Messages Accumulate in DLQ

- **Cause:** Fargate task is failing to process messages
- **Steps:**
  1. Check Fargate task logs in CloudWatch Logs (`/ecs/adapty-ct-connector/{account_id}`)
  2. Look for errors related to CleverTap API (invalid passcode, region, etc.)
  3. Fix the issue and manually replay DLQ messages via AWS CLI:
     ```bash
     aws sqs receive-message --queue-url {dlq-url} --max-number-of-messages 10
     # Copy messages back to main queue
     aws sqs send-message --queue-url {main-queue-url} --message-body '{message}'
     ```

### Task Health Check Failing

- **Cause:** Connector not listening on port 8080 or `/healthz` endpoint not responding
- **Fix:** Check task logs; ensure connector binary is running and health check endpoint is implemented

### Queue Depth Growing

- **Cause:** Processing rate slower than ingestion rate
- **Fix:** Monitor CloudWatch metrics; if sustained, increase `BATCH_SIZE` or add another Fargate task

---

## Support & References

- **Adapty API Documentation:** https://adapty.io/developers
- **AWS SQS Documentation:** https://docs.aws.amazon.com/sqs/
- **AWS Fargate Documentation:** https://docs.aws.amazon.com/fargate/
- **CleverTap Event Upload API:** https://clevertap.com/developers/rest-api/
