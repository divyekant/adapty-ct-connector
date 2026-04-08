# Adapty-to-CleverTap Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go service that polls SQS for Adapty webhook events, transforms them, and uploads to CleverTap's Upload Events API — plus a backfill CLI and local dev stack.

**Architecture:** Fargate container long-polls per-account SQS queues. A shared `transform` package converts Adapty payloads into CleverTap event format across 7 configurable layers. Same transform logic powers the backfill CLI. Local dev uses Docker Compose with LocalStack (SQS) and a mock CleverTap server.

**Tech Stack:** Go 1.25, AWS SDK v2 (SQS), `slog` for structured logging, `hashicorp/golang-lru` for dedup cache, Docker + LocalStack for local dev.

**Spec:** `docs/superpowers/specs/2026-03-23-adapty-ct-connector-design.md`

---

## File Map

| File | Responsibility |
|---|---|
| `go.mod` | Module definition, dependencies |
| `internal/adapty/types.go` | Adapty webhook payload Go structs |
| `internal/clevertap/types.go` | CleverTap Upload Events API request/response Go structs |
| `internal/clevertap/client.go` | CleverTap HTTP client — upload, retry, response parsing |
| `internal/transform/config.go` | TransformConfig struct, JSON loading, layer/field exclusion logic |
| `internal/transform/transform.go` | Adapty → CleverTap event mapping (7 layers, identity, ts, null handling) |
| `internal/queue/consumer.go` | SQS long-poll loop, batching, dedup, graceful shutdown, health check |
| `cmd/connector/main.go` | Fargate entrypoint — wires config, SQS consumer, CT client, health server |
| `cmd/backfill/main.go` | Backfill CLI — NDJSON reader, batching, concurrency, progress, dry-run |
| `cmd/mock-clevertap/main.go` | Local dev mock CT server — validates and logs payloads |
| `internal/queue/sqs_adapter.go` | AWS SDK SQS wrapper implementing SQSClient interface |
| `testdata/subscription_started.json` | Sample Adapty payload for tests and scripts |
| `testdata/access_level_updated.json` | Sample payload with access_level_updated-specific fields |
| `testdata/full_payload.json` | Full payload with all 7 layers populated |
| `scripts/send-webhook.sh` | Sends a single test event to local SQS |
| `scripts/seed-queue.sh` | Bulk-loads test events for integration testing |
| `docker-compose.yml` | LocalStack + connector + mock-clevertap |
| `Dockerfile` | Multi-stage build for connector binary |
| `Dockerfile.mock` | Build for mock CleverTap server |
| `scripts/init-localstack.sh` | LocalStack startup script to create SQS queues |
| `transform-config.json` | Default config (all layers enabled, no exclusions) |
| `docs/architecture.md` | Infra team handoff document |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `go.mod`
- Create: `internal/adapty/types.go`
- Create: `internal/clevertap/types.go`

- [ ] **Step 1: Initialize Go module**

```bash
cd /Users/dk/projects/adapty-ct-connector
go mod init github.com/anthropic/adapty-ct-connector
```

- [ ] **Step 2: Create Adapty types**

Create `internal/adapty/types.go` with Go structs matching the Adapty webhook payload:

```go
package adapty

// Event represents the top-level Adapty webhook payload.
type Event struct {
	ProfileID                  string                 `json:"profile_id"`
	CustomerUserID             *string                `json:"customer_user_id"`
	IDFV                       *string                `json:"idfv"`
	IDFA                       *string                `json:"idfa"`
	AdvertisingID              *string                `json:"advertising_id"`
	ProfileInstallDatetime     *string                `json:"profile_install_datetime"`
	UserAgent                  *string                `json:"user_agent"`
	Email                      *string                `json:"email"`
	EventType                  string                 `json:"event_type"`
	EventDatetime              string                 `json:"event_datetime"`
	EventProperties            map[string]interface{} `json:"event_properties"`
	EventAPIVersion            *int                   `json:"event_api_version"`
	ProfilesSharingAccessLevel []ProfileShare         `json:"profiles_sharing_access_level"`
	Attributions               map[string]Attribution `json:"attributions"`
	UserAttributes             map[string]interface{} `json:"user_attributes"`
	IntegrationIDs             map[string]string      `json:"integration_ids"`
	PlayStorePurchaseToken     *PlayStorePurchaseToken `json:"play_store_purchase_token"`
}

// ProfileShare represents a profile sharing an access level.
type ProfileShare struct {
	ProfileID      string  `json:"profile_id"`
	CustomerUserID *string `json:"customer_user_id"`
}

// Attribution represents attribution data from a single source.
type Attribution struct {
	AdSet         *string `json:"ad_set"`
	Status        *string `json:"status"`
	Channel       *string `json:"channel"`
	AdGroup       *string `json:"ad_group"`
	Campaign      *string `json:"campaign"`
	Creative      *string `json:"creative"`
	CreatedAt     *string `json:"created_at"`
	NetworkUserID *string `json:"network_user_id"`
}

// PlayStorePurchaseToken contains Google Play purchase validation data.
type PlayStorePurchaseToken struct {
	ProductID      string `json:"product_id"`
	PurchaseToken  string `json:"purchase_token"`
	IsSubscription bool   `json:"is_subscription"`
}
```

Use `*string` for nullable string fields (Adapty sends `null` for missing values). Use `map[string]interface{}` for `event_properties` because the field set varies by event type and we forward everything dynamically.

- [ ] **Step 3: Create CleverTap types**

Create `internal/clevertap/types.go`:

```go
package clevertap

// UploadRequest is the payload sent to CleverTap's /1/upload endpoint.
type UploadRequest struct {
	D []EventRecord `json:"d"`
}

// EventRecord is a single event in the upload batch.
type EventRecord struct {
	Identity string                 `json:"identity"`
	TS       int64                  `json:"ts"`
	Type     string                 `json:"type"`
	EvtName  string                 `json:"evtName"`
	EvtData  map[string]interface{} `json:"evtData"`
}

// UploadResponse is CleverTap's response to an upload request.
type UploadResponse struct {
	Status      string        `json:"status"` // "success", "partial", "fail"
	Processed   int           `json:"processed"`
	Unprocessed []Unprocessed `json:"unprocessed"`
}

// Unprocessed describes a record that CleverTap failed to process.
type Unprocessed struct {
	Status  string `json:"status"`
	Code    int    `json:"code"`
	Error   string `json:"error"`
	Record  int    `json:"record"`
}
```

- [ ] **Step 4: Verify it compiles**

```bash
cd /Users/dk/projects/adapty-ct-connector
go build ./internal/adapty/... && go build ./internal/clevertap/...
```

Expected: No output (success).

- [ ] **Step 5: Commit**

```bash
git add go.mod internal/adapty/types.go internal/clevertap/types.go
git commit -m "feat: add project scaffolding with Adapty and CleverTap types"
```

---

## Task 2: Transform Config

**Files:**
- Create: `internal/transform/config.go`
- Create: `internal/transform/config_test.go`
- Create: `transform-config.json`

- [ ] **Step 1: Write config tests**

Create `internal/transform/config_test.go`:

```go
package transform

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_Default(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.IsLayerDisabled("top_level") {
		t.Error("top_level should be enabled by default")
	}
	if cfg.IsFieldExcluded("event_properties", "store") {
		t.Error("store should not be excluded by default")
	}
}

func TestLoadConfig_FromFile(t *testing.T) {
	content := `{"disabled_layers":["play_store"],"excluded_fields":{"top_level":["user_agent"]}}`
	path := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(path, []byte(content), 0644)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.IsLayerDisabled("play_store") {
		t.Error("play_store should be disabled")
	}
	if !cfg.IsFieldExcluded("top_level", "user_agent") {
		t.Error("user_agent should be excluded from top_level")
	}
	if cfg.IsLayerDisabled("event_properties") {
		t.Error("event_properties should not be disabled")
	}
}

func TestLoadConfig_EmptyPath(t *testing.T) {
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IsLayerDisabled("top_level") {
		t.Error("should return default config for empty path")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/transform/... -v
```

Expected: FAIL — package doesn't exist yet.

- [ ] **Step 3: Implement config**

Create `internal/transform/config.go`:

```go
package transform

import (
	"encoding/json"
	"os"
)

// Config controls which layers and fields are included in the transformation.
type Config struct {
	DisabledLayers []string            `json:"disabled_layers"`
	ExcludedFields map[string][]string `json:"excluded_fields"`

	disabledSet map[string]bool
	excludedSet map[string]map[string]bool
}

// DefaultConfig returns a config with all layers enabled and no exclusions.
func DefaultConfig() *Config {
	return &Config{
		disabledSet: make(map[string]bool),
		excludedSet: make(map[string]map[string]bool),
	}
}

// LoadConfig loads config from a JSON file. Returns DefaultConfig if path is empty.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return DefaultConfig(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.buildLookups()
	return &cfg, nil
}

func (c *Config) buildLookups() {
	c.disabledSet = make(map[string]bool, len(c.DisabledLayers))
	for _, l := range c.DisabledLayers {
		c.disabledSet[l] = true
	}
	c.excludedSet = make(map[string]map[string]bool, len(c.ExcludedFields))
	for layer, fields := range c.ExcludedFields {
		s := make(map[string]bool, len(fields))
		for _, f := range fields {
			s[f] = true
		}
		c.excludedSet[layer] = s
	}
}

// IsLayerDisabled returns true if the given layer name is disabled.
func (c *Config) IsLayerDisabled(layer string) bool {
	return c.disabledSet[layer]
}

// IsFieldExcluded returns true if the given field in the given layer is excluded.
func (c *Config) IsFieldExcluded(layer, field string) bool {
	if s, ok := c.excludedSet[layer]; ok {
		return s[field]
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/transform/... -v
```

Expected: PASS (3 tests).

- [ ] **Step 5: Create default transform-config.json**

Create `transform-config.json` at project root:

```json
{
  "disabled_layers": [],
  "excluded_fields": {}
}
```

- [ ] **Step 6: Commit**

```bash
git add internal/transform/config.go internal/transform/config_test.go transform-config.json
git commit -m "feat: add transformation config with layer/field exclusion"
```

---

## Task 3: Core Transformation Logic

**Files:**
- Create: `internal/transform/transform.go`
- Create: `internal/transform/transform_test.go`
- Create: `testdata/subscription_started.json`
- Create: `testdata/access_level_updated.json`
- Create: `testdata/full_payload.json`

- [ ] **Step 1: Create test data files**

Create `testdata/subscription_started.json`:

```json
{
  "profile_id": "abc-123",
  "customer_user_id": "user_42",
  "email": "john@example.com",
  "event_type": "subscription_started",
  "event_datetime": "2026-03-23T10:00:00.000000+0000",
  "event_properties": {
    "store": "play_store",
    "currency": "USD",
    "price_usd": 4.99,
    "vendor_product_id": "premium_monthly",
    "profile_event_id": "evt-789",
    "environment": "Production",
    "profile_id": "abc-123",
    "event_datetime": "2026-03-23T10:00:00.000000+0000"
  },
  "attributions": {
    "appsflyer": {
      "campaign": "spring_promo",
      "status": "non_organic"
    }
  },
  "user_attributes": {"plan_preference": "annual"},
  "integration_ids": {"firebase_app_instance_id": "fb-123"},
  "event_api_version": 1
}
```

Create `testdata/access_level_updated.json`:

```json
{
  "profile_id": "def-456",
  "customer_user_id": null,
  "event_type": "access_level_updated",
  "event_datetime": "2026-03-23T11:00:00.000000+0000",
  "event_properties": {
    "access_level_id": "premium",
    "is_active": true,
    "will_renew": true,
    "expires_at": "2026-04-23T11:00:00.000000+0000",
    "vendor_product_id": "premium_monthly",
    "environment": "Production",
    "profile_event_id": "evt-790"
  },
  "event_api_version": 1
}
```

Create `testdata/full_payload.json` (all 7 layers populated):

```json
{
  "profile_id": "full-001",
  "customer_user_id": "user_full",
  "idfv": "idfv-001",
  "idfa": "idfa-001",
  "advertising_id": "adid-001",
  "profile_install_datetime": "2026-01-01T00:00:00.000000+0000",
  "user_agent": "TestAgent/1.0",
  "email": "full@example.com",
  "event_type": "subscription_renewed",
  "event_datetime": "2026-03-23T12:00:00.000000+0000",
  "event_properties": {
    "store": "app_store",
    "currency": "EUR",
    "price_usd": 9.99,
    "price_local": 8.99,
    "vendor_product_id": "annual_plan",
    "transaction_id": "txn-001",
    "original_transaction_id": "txn-000",
    "purchase_date": "2026-03-23T12:00:00.000000+0000",
    "original_purchase_date": "2025-03-23T12:00:00.000000+0000",
    "subscription_expires_at": "2027-03-23T12:00:00.000000+0000",
    "profile_event_id": "evt-full",
    "profile_id": "full-001",
    "profile_country": "DE",
    "profile_ip_address": "10.0.0.1",
    "profile_has_access_level": true,
    "profile_total_revenue_usd": 19.98,
    "environment": "Production",
    "consecutive_payments": 2,
    "rate_after_first_year": false,
    "store_country": "DE",
    "cohort_name": "All Users",
    "paywall_name": "MainPaywall",
    "paywall_revision": "3",
    "variation_id": "var-001",
    "developer_id": "main_placement",
    "ab_test_name": "pricing_test",
    "ab_test_revision": 2,
    "base_plan_id": "annual",
    "promotional_offer_id": null,
    "store_offer_category": null,
    "store_offer_discount_type": null,
    "event_datetime": "2026-03-23T12:00:00.000000+0000",
    "net_revenue_usd": 8.49,
    "net_revenue_local": 7.64,
    "proceeds_usd": 8.49,
    "proceeds_local": 7.64,
    "tax_amount_usd": 0,
    "tax_amount_local": 0
  },
  "event_api_version": 1,
  "profiles_sharing_access_level": [
    {"profile_id": "shared-001", "customer_user_id": "shared_user"}
  ],
  "attributions": {
    "appsflyer": {
      "campaign": "winter_sale",
      "status": "non_organic",
      "channel": "Google Ads",
      "ad_set": "Keywords 1",
      "ad_group": "Group A",
      "creative": "banner_1",
      "created_at": "2026-01-15T00:00:00.000000+0000",
      "network_user_id": "nuid-001"
    }
  },
  "user_attributes": {"tier": "gold", "lang": "de"},
  "integration_ids": {"firebase_app_instance_id": "fb-full", "branch_id": "br-full"},
  "play_store_purchase_token": {
    "product_id": "annual_plan",
    "purchase_token": "token-full",
    "is_subscription": true
  }
}
```

- [ ] **Step 2: Write transform tests**

Create `internal/transform/transform_test.go`:

```go
package transform

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
)

func loadTestEvent(t *testing.T, path string) adapty.Event {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test data %s: %v", path, err)
	}
	var evt adapty.Event
	if err := json.Unmarshal(data, &evt); err != nil {
		t.Fatalf("failed to unmarshal test data: %v", err)
	}
	return evt
}

func TestTransform_Identity_CustomerUserID(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/subscription_started.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Identity != "user_42" {
		t.Errorf("expected identity 'user_42', got '%s'", rec.Identity)
	}
}

func TestTransform_Identity_FallbackToProfileID(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/access_level_updated.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Identity != "def-456" {
		t.Errorf("expected identity 'def-456', got '%s'", rec.Identity)
	}
}

func TestTransform_EventName_Known(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/subscription_started.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtName != "Subscription Started" {
		t.Errorf("expected 'Subscription Started', got '%s'", rec.EvtName)
	}
}

func TestTransform_EventName_Unknown(t *testing.T) {
	evt := adapty.Event{ProfileID: "p1", EventType: "some_future_event", EventDatetime: "2026-03-23T10:00:00.000000+0000"}
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtName != "Adapty some_future_event" {
		t.Errorf("expected 'Adapty some_future_event', got '%s'", rec.EvtName)
	}
}

func TestTransform_Timestamp(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/subscription_started.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2026-03-23T10:00:00 UTC = 1774436400
	if rec.TS != 1774436400 {
		t.Errorf("expected ts 1774436400, got %d", rec.TS)
	}
}

func TestTransform_NullFieldsOmitted(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// promotional_offer_id is null in test data — should not appear
	if _, ok := rec.EvtData["promotional_offer_id"]; ok {
		t.Error("null field 'promotional_offer_id' should be omitted")
	}
}

func TestTransform_Layer2OverridesLayer1(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/subscription_started.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// profile_id exists in both top-level and event_properties; event_properties wins
	pid, ok := rec.EvtData["profile_id"]
	if !ok {
		t.Fatal("expected profile_id in evtData")
	}
	if pid != "abc-123" {
		t.Errorf("expected 'abc-123', got '%v'", pid)
	}
}

func TestTransform_Attributions(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtData["attribution_appsflyer_campaign"] != "winter_sale" {
		t.Errorf("expected attribution_appsflyer_campaign 'winter_sale', got '%v'", rec.EvtData["attribution_appsflyer_campaign"])
	}
	if rec.EvtData["attribution_appsflyer_channel"] != "Google Ads" {
		t.Errorf("expected attribution_appsflyer_channel 'Google Ads', got '%v'", rec.EvtData["attribution_appsflyer_channel"])
	}
}

func TestTransform_UserAttributes(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtData["user_attr_tier"] != "gold" {
		t.Errorf("expected user_attr_tier 'gold', got '%v'", rec.EvtData["user_attr_tier"])
	}
}

func TestTransform_IntegrationIDs(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtData["integration_firebase_app_instance_id"] != "fb-full" {
		t.Errorf("expected integration_firebase_app_instance_id 'fb-full', got '%v'", rec.EvtData["integration_firebase_app_instance_id"])
	}
}

func TestTransform_PlayStorePurchaseToken(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.EvtData["play_store_product_id"] != "annual_plan" {
		t.Errorf("expected play_store_product_id 'annual_plan', got '%v'", rec.EvtData["play_store_product_id"])
	}
	if rec.EvtData["play_store_is_subscription"] != true {
		t.Errorf("expected play_store_is_subscription true, got '%v'", rec.EvtData["play_store_is_subscription"])
	}
}

func TestTransform_ProfilesSharing(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, ok := rec.EvtData["profiles_sharing_access_level"]
	if !ok {
		t.Fatal("expected profiles_sharing_access_level")
	}
	str, ok := val.(string)
	if !ok {
		t.Fatalf("expected string, got %T", val)
	}
	if str == "" || str == "null" {
		t.Error("expected non-empty JSON string")
	}
}

func TestTransform_DisabledLayer(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg, _ := LoadConfig("")
	cfg.DisabledLayers = []string{"attributions", "play_store"}
	cfg.buildLookups()

	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := rec.EvtData["attribution_appsflyer_campaign"]; ok {
		t.Error("attributions layer should be disabled")
	}
	if _, ok := rec.EvtData["play_store_product_id"]; ok {
		t.Error("play_store layer should be disabled")
	}
	// Other layers should still work
	if _, ok := rec.EvtData["user_attr_tier"]; !ok {
		t.Error("user_attributes layer should still be active")
	}
}

func TestTransform_ExcludedField(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/full_payload.json")
	cfg := DefaultConfig()
	cfg.ExcludedFields = map[string][]string{
		"top_level":        {"user_agent", "email"},
		"event_properties": {"profile_ip_address"},
	}
	cfg.buildLookups()

	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := rec.EvtData["user_agent"]; ok {
		t.Error("user_agent should be excluded")
	}
	if _, ok := rec.EvtData["email"]; ok {
		t.Error("email should be excluded")
	}
	if _, ok := rec.EvtData["profile_ip_address"]; ok {
		t.Error("profile_ip_address should be excluded")
	}
	// Non-excluded fields should still be present
	if _, ok := rec.EvtData["store"]; !ok {
		t.Error("store should still be present")
	}
}

func TestTransform_EmptyEventType(t *testing.T) {
	evt := adapty.Event{ProfileID: "p1", EventType: ""}
	cfg := DefaultConfig()
	_, err := Transform(evt, cfg)
	if err == nil {
		t.Error("expected error for empty event_type")
	}
}

func TestTransform_Type(t *testing.T) {
	evt := loadTestEvent(t, "../../testdata/subscription_started.json")
	cfg := DefaultConfig()
	rec, err := Transform(evt, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Type != "event" {
		t.Errorf("expected type 'event', got '%s'", rec.Type)
	}
}

func TestTransformBatch(t *testing.T) {
	evt1 := loadTestEvent(t, "../../testdata/subscription_started.json")
	evt2 := loadTestEvent(t, "../../testdata/access_level_updated.json")
	cfg := DefaultConfig()

	records, errs := TransformBatch([]adapty.Event{evt1, evt2}, cfg)
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
	if len(errs) != 0 {
		t.Errorf("expected 0 errors, got %d", len(errs))
	}
	if records[0].EvtName != "Subscription Started" {
		t.Errorf("expected 'Subscription Started', got '%s'", records[0].EvtName)
	}
	if records[1].EvtName != "Access Level Updated" {
		t.Errorf("expected 'Access Level Updated', got '%s'", records[1].EvtName)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/transform/... -v -run 'TestTransform_'
```

Expected: FAIL — `Transform` and `TransformBatch` functions don't exist.

- [ ] **Step 4: Implement transformation**

Create `internal/transform/transform.go`:

```go
package transform

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
)

// eventNameMap maps Adapty event_type to CleverTap evtName.
var eventNameMap = map[string]string{
	"subscription_started":               "Subscription Started",
	"subscription_renewed":               "Subscription Renewed",
	"subscription_renewal_cancelled":     "Subscription Renewal Cancelled",
	"subscription_renewal_reactivated":   "Subscription Renewal Reactivated",
	"subscription_expired":               "Subscription Expired",
	"subscription_paused":                "Subscription Paused",
	"subscription_deferred":              "Subscription Deferred",
	"subscription_refunded":              "Subscription Refunded",
	"non_subscription_purchase":          "Non Subscription Purchase",
	"non_subscription_purchase_refunded": "Non Subscription Purchase Refunded",
	"trial_started":                      "Trial Started",
	"trial_converted":                    "Trial Converted",
	"trial_renewal_cancelled":            "Trial Renewal Cancelled",
	"trial_renewal_reactivated":          "Trial Renewal Reactivated",
	"trial_expired":                      "Trial Expired",
	"entered_grace_period":               "Entered Grace Period",
	"billing_issue_detected":             "Billing Issue Detected",
	"access_level_updated":               "Access Level Updated",
}

// Transform converts a single Adapty event into a CleverTap EventRecord.
func Transform(evt adapty.Event, cfg *Config) (clevertap.EventRecord, error) {
	if evt.EventType == "" {
		return clevertap.EventRecord{}, errors.New("missing event_type")
	}

	rec := clevertap.EventRecord{
		Identity: resolveIdentity(evt),
		TS:       parseTimestamp(evt.EventDatetime),
		Type:     "event",
		EvtName:  mapEventName(evt.EventType),
		EvtData:  make(map[string]interface{}),
	}

	// Layer 1: Top-level fields
	if !cfg.IsLayerDisabled("top_level") {
		addStringIfPresent(rec.EvtData, "profile_id", evt.ProfileID, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "idfv", evt.IDFV, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "idfa", evt.IDFA, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "advertising_id", evt.AdvertisingID, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "profile_install_datetime", evt.ProfileInstallDatetime, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "user_agent", evt.UserAgent, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "email", evt.Email, cfg, "top_level")
		addPtrIfPresent(rec.EvtData, "event_api_version", evt.EventAPIVersion, cfg, "top_level")
	}

	// Layer 2: Event properties (overrides Layer 1 on collision)
	if !cfg.IsLayerDisabled("event_properties") {
		for k, v := range evt.EventProperties {
			if v == nil || cfg.IsFieldExcluded("event_properties", k) {
				continue
			}
			rec.EvtData[k] = v
		}
	}

	// Layer 3: Attributions
	if !cfg.IsLayerDisabled("attributions") && evt.Attributions != nil {
		for source, attr := range evt.Attributions {
			flattenAttribution(rec.EvtData, source, attr, cfg)
		}
	}

	// Layer 4: User attributes
	if !cfg.IsLayerDisabled("user_attributes") && evt.UserAttributes != nil {
		for k, v := range evt.UserAttributes {
			if v == nil || cfg.IsFieldExcluded("user_attributes", k) {
				continue
			}
			rec.EvtData["user_attr_"+k] = v
		}
	}

	// Layer 5: Integration IDs
	if !cfg.IsLayerDisabled("integration_ids") && evt.IntegrationIDs != nil {
		for k, v := range evt.IntegrationIDs {
			if cfg.IsFieldExcluded("integration_ids", k) {
				continue
			}
			rec.EvtData["integration_"+k] = v
		}
	}

	// Layer 6: Play Store purchase token
	if !cfg.IsLayerDisabled("play_store") && evt.PlayStorePurchaseToken != nil {
		pt := evt.PlayStorePurchaseToken
		if !cfg.IsFieldExcluded("play_store", "product_id") {
			rec.EvtData["play_store_product_id"] = pt.ProductID
		}
		if !cfg.IsFieldExcluded("play_store", "purchase_token") {
			rec.EvtData["play_store_purchase_token"] = pt.PurchaseToken
		}
		if !cfg.IsFieldExcluded("play_store", "is_subscription") {
			rec.EvtData["play_store_is_subscription"] = pt.IsSubscription
		}
	}

	// Layer 7: Profiles sharing access level
	if !cfg.IsLayerDisabled("profiles_sharing") && len(evt.ProfilesSharingAccessLevel) > 0 {
		data, _ := json.Marshal(evt.ProfilesSharingAccessLevel)
		rec.EvtData["profiles_sharing_access_level"] = string(data)
	}

	return rec, nil
}

// TransformBatch converts multiple Adapty events. Returns successful records and any errors.
func TransformBatch(events []adapty.Event, cfg *Config) ([]clevertap.EventRecord, []error) {
	var records []clevertap.EventRecord
	var errs []error
	for _, evt := range events {
		rec, err := Transform(evt, cfg)
		if err != nil {
			errs = append(errs, fmt.Errorf("event %s: %w", evt.EventType, err))
			continue
		}
		records = append(records, rec)
	}
	return records, errs
}

func resolveIdentity(evt adapty.Event) string {
	if evt.CustomerUserID != nil && *evt.CustomerUserID != "" {
		return *evt.CustomerUserID
	}
	return evt.ProfileID
}

func mapEventName(eventType string) string {
	if name, ok := eventNameMap[eventType]; ok {
		return name
	}
	return "Adapty " + eventType
}

func parseTimestamp(datetime string) int64 {
	// Adapty format: "2026-03-23T10:00:00.000000+0000"
	formats := []string{
		"2006-01-02T15:04:05.000000-0700",
		"2006-01-02T15:04:05.000000+0000",
		"2006-01-02T15:04:05-0700",
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, datetime); err == nil {
			return t.Unix()
		}
	}
	return 0
}

func addStringIfPresent(data map[string]interface{}, key string, val string, cfg *Config, layer string) {
	if cfg.IsFieldExcluded(layer, key) || val == "" {
		return
	}
	data[key] = val
}

func addPtrIfPresent[T any](data map[string]interface{}, key string, val *T, cfg *Config, layer string) {
	if cfg.IsFieldExcluded(layer, key) || val == nil {
		return
	}
	data[key] = *val
}

func flattenAttribution(data map[string]interface{}, source string, attr adapty.Attribution, cfg *Config) {
	prefix := "attribution_" + source + "_"
	fields := map[string]*string{
		"campaign":        attr.Campaign,
		"status":          attr.Status,
		"channel":         attr.Channel,
		"ad_set":          attr.AdSet,
		"ad_group":        attr.AdGroup,
		"creative":        attr.Creative,
		"created_at":      attr.CreatedAt,
		"network_user_id": attr.NetworkUserID,
	}
	for k, v := range fields {
		if v == nil || cfg.IsFieldExcluded("attributions", k) {
			continue
		}
		data[prefix+k] = *v
	}
}

```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/transform/... -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/transform/transform.go internal/transform/transform_test.go testdata/
git commit -m "feat: add core transformation logic with 7-layer flattening"
```

---

## Task 4: CleverTap Client

**Files:**
- Create: `internal/clevertap/client.go`
- Create: `internal/clevertap/client_test.go`

- [ ] **Step 1: Write client tests**

Create `internal/clevertap/client_test.go`:

```go
package clevertap

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_Upload_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify headers
		if r.Header.Get("X-CleverTap-Account-Id") != "test-account" {
			t.Errorf("missing account header")
		}
		if r.Header.Get("X-CleverTap-Passcode") != "test-pass" {
			t.Errorf("missing passcode header")
		}
		if r.Header.Get("Content-Type") != "application/json; charset=utf-8" {
			t.Errorf("wrong content type: %s", r.Header.Get("Content-Type"))
		}
		json.NewEncoder(w).Encode(UploadResponse{Status: "success", Processed: 1})
	}))
	defer server.Close()

	c := NewClient("test-account", "test-pass", server.URL)
	resp, err := c.Upload(UploadRequest{
		D: []EventRecord{{Identity: "u1", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "success" {
		t.Errorf("expected success, got %s", resp.Status)
	}
}

func TestClient_Upload_Partial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(UploadResponse{
			Status:    "partial",
			Processed: 1,
			Unprocessed: []Unprocessed{
				{Status: "fail", Code: 513, Error: "invalid identity", Record: 1},
			},
		})
	}))
	defer server.Close()

	c := NewClient("acc", "pass", server.URL)
	resp, err := c.Upload(UploadRequest{
		D: []EventRecord{
			{Identity: "u1", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}},
			{Identity: "", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "partial" {
		t.Errorf("expected partial, got %s", resp.Status)
	}
	if len(resp.Unprocessed) != 1 {
		t.Errorf("expected 1 unprocessed, got %d", len(resp.Unprocessed))
	}
}

func TestClient_Upload_Retries5xx(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(UploadResponse{Status: "success", Processed: 1})
	}))
	defer server.Close()

	c := NewClient("acc", "pass", server.URL)
	c.MaxRetries = 3
	c.InitialBackoff = 0 // no delay in tests
	resp, err := c.Upload(UploadRequest{
		D: []EventRecord{{Identity: "u1", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "success" {
		t.Errorf("expected success after retries, got %s", resp.Status)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestClient_Upload_AuthFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	c := NewClient("bad", "creds", server.URL)
	_, err := c.Upload(UploadRequest{
		D: []EventRecord{{Identity: "u1", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}}},
	})
	if err == nil {
		t.Fatal("expected auth error")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.StatusCode != 401 {
		t.Errorf("expected status 401, got %d", authErr.StatusCode)
	}
}

func TestClient_Upload_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	c := NewClient("acc", "pass", server.URL)
	_, err := c.Upload(UploadRequest{
		D: []EventRecord{{Identity: "u1", TS: 123, Type: "event", EvtName: "Test", EvtData: map[string]interface{}{}}},
	})
	if err == nil {
		t.Fatal("expected error for 400")
	}
}

func TestNewClientFromRegion(t *testing.T) {
	c := NewClientFromRegion("acc", "pass", "eu1")
	if c.baseURL != "https://eu1.api.clevertap.com/1/upload" {
		t.Errorf("wrong URL: %s", c.baseURL)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/clevertap/... -v
```

Expected: FAIL — `NewClient`, `NewClientFromRegion`, etc. don't exist.

- [ ] **Step 3: Implement client**

Create `internal/clevertap/client.go`:

```go
package clevertap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// AuthError is returned when CleverTap rejects credentials (401/403).
type AuthError struct {
	StatusCode int
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("clevertap auth failure: HTTP %d", e.StatusCode)
}

// Client sends events to CleverTap's Upload API.
type Client struct {
	accountID      string
	passcode       string
	baseURL        string
	httpClient     *http.Client
	MaxRetries     int
	InitialBackoff time.Duration
}

// NewClient creates a client with a custom base URL (for testing).
func NewClient(accountID, passcode, baseURL string) *Client {
	return &Client{
		accountID:      accountID,
		passcode:       passcode,
		baseURL:        baseURL,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		MaxRetries:     3,
		InitialBackoff: 1 * time.Second,
	}
}

// NewClientFromRegion creates a client using a CleverTap region code.
func NewClientFromRegion(accountID, passcode, region string) *Client {
	url := fmt.Sprintf("https://%s.api.clevertap.com/1/upload", region)
	return NewClient(accountID, passcode, url)
}

// Upload sends an upload request to CleverTap with retry logic.
func (c *Client) Upload(req UploadRequest) (*UploadResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 && c.InitialBackoff > 0 {
			backoff := c.InitialBackoff * (1 << (attempt - 1))
			time.Sleep(backoff)
		}

		resp, err := c.doRequest(body)
		if err != nil {
			lastErr = err
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (c *Client) doRequest(body []byte) (*UploadResponse, error) {
	req, err := http.NewRequest("POST", c.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-CleverTap-Account-Id", c.accountID)
	req.Header.Set("X-CleverTap-Passcode", c.passcode)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Auth failures are not retryable
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return nil, &AuthError{StatusCode: resp.StatusCode}
	}

	// 4xx (not auth) are not retryable
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, fmt.Errorf("clevertap HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	// 5xx and 429 are retryable
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return nil, fmt.Errorf("clevertap HTTP %d (retryable): %s", resp.StatusCode, string(respBody))
	}

	var uploadResp UploadResponse
	if err := json.Unmarshal(respBody, &uploadResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &uploadResp, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/clevertap/... -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/clevertap/client.go internal/clevertap/client_test.go
git commit -m "feat: add CleverTap API client with retry and auth error handling"
```

---

## Task 5: SQS Consumer

**Files:**
- Create: `internal/queue/consumer.go`
- Create: `internal/queue/consumer_test.go`

- [ ] **Step 1: Add AWS SDK dependency**

```bash
cd /Users/dk/projects/adapty-ct-connector
go get github.com/aws/aws-sdk-go-v2/service/sqs
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/hashicorp/golang-lru/v2
```

- [ ] **Step 2: Write consumer tests**

Create `internal/queue/consumer_test.go`:

```go
package queue

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

// mockSQS implements the minimal SQS interface for testing.
type mockSQS struct {
	messages []string
	deleted  []string
}

func (m *mockSQS) ReceiveMessages(ctx context.Context, maxMessages int) ([]Message, error) {
	if len(m.messages) == 0 {
		return nil, nil
	}
	var msgs []Message
	for i, body := range m.messages {
		if i >= maxMessages {
			break
		}
		msgs = append(msgs, Message{
			Body:          body,
			ReceiptHandle: "handle-" + body[:10],
			MessageID:     "msg-" + body[:10],
		})
	}
	m.messages = m.messages[len(msgs):]
	return msgs, nil
}

func (m *mockSQS) DeleteMessage(ctx context.Context, receiptHandle string) error {
	m.deleted = append(m.deleted, receiptHandle)
	return nil
}

// mockCT implements the minimal CleverTap interface for testing.
type mockCT struct {
	uploaded []clevertap.UploadRequest
	response *clevertap.UploadResponse
	err      error
}

func (m *mockCT) Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error) {
	m.uploaded = append(m.uploaded, req)
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func makeTestMessage(eventType, profileID string) string {
	uid := "user_1"
	evt := adapty.Event{
		ProfileID:      profileID,
		CustomerUserID: &uid,
		EventType:      eventType,
		EventDatetime:  "2026-03-23T10:00:00.000000+0000",
		EventProperties: map[string]interface{}{
			"profile_event_id": profileID + "-evt",
			"environment":      "Production",
		},
	}
	data, _ := json.Marshal(evt)
	return string(data)
}

func TestConsumer_ProcessBatch(t *testing.T) {
	sqs := &mockSQS{
		messages: []string{
			makeTestMessage("subscription_started", "p1"),
			makeTestMessage("trial_started", "p2"),
		},
	}
	ct := &mockCT{
		response: &clevertap.UploadResponse{Status: "success", Processed: 2},
	}
	cfg := transform.DefaultConfig()
	consumer := NewConsumer(sqs, ct, cfg, 10, 1000)

	processed, err := consumer.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 2 {
		t.Errorf("expected 2 processed, got %d", processed)
	}
	if len(ct.uploaded) != 1 {
		t.Errorf("expected 1 upload call (batched), got %d", len(ct.uploaded))
	}
	if len(ct.uploaded[0].D) != 2 {
		t.Errorf("expected 2 records in batch, got %d", len(ct.uploaded[0].D))
	}
	if len(sqs.deleted) != 2 {
		t.Errorf("expected 2 deleted, got %d", len(sqs.deleted))
	}
}

func TestConsumer_SkipsDuplicates(t *testing.T) {
	sqs := &mockSQS{
		messages: []string{
			makeTestMessage("subscription_started", "p1"),
			makeTestMessage("subscription_started", "p1"), // same profile_event_id
		},
	}
	ct := &mockCT{
		response: &clevertap.UploadResponse{Status: "success", Processed: 1},
	}
	cfg := transform.DefaultConfig()
	consumer := NewConsumer(sqs, ct, cfg, 10, 1000)

	processed, err := consumer.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 processed (1 deduped), got %d", processed)
	}
}

func TestConsumer_SkipsEmptyEventType(t *testing.T) {
	sqs := &mockSQS{
		messages: []string{`{}`},
	}
	ct := &mockCT{
		response: &clevertap.UploadResponse{Status: "success", Processed: 0},
	}
	cfg := transform.DefaultConfig()
	consumer := NewConsumer(sqs, ct, cfg, 10, 1000)

	processed, err := consumer.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 0 {
		t.Errorf("expected 0 processed for empty event_type, got %d", processed)
	}
}

func TestConsumer_GracefulShutdown(t *testing.T) {
	sqs := &mockSQS{
		messages: []string{makeTestMessage("subscription_started", "p1")},
	}
	ct := &mockCT{
		response: &clevertap.UploadResponse{Status: "success", Processed: 1},
	}
	cfg := transform.DefaultConfig()
	consumer := NewConsumer(sqs, ct, cfg, 10, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	var loopCount atomic.Int32

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	consumer.Run(ctx, func() { loopCount.Add(1) })

	if loopCount.Load() < 1 {
		t.Error("expected at least 1 loop iteration before shutdown")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/queue/... -v
```

Expected: FAIL — `Consumer`, `NewConsumer`, etc. don't exist.

- [ ] **Step 4: Implement consumer**

Create `internal/queue/consumer.go`:

```go
package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

// Message represents a message from the queue.
type Message struct {
	Body          string
	ReceiptHandle string
	MessageID     string
}

// SQSClient is the interface for SQS operations used by the consumer.
type SQSClient interface {
	ReceiveMessages(ctx context.Context, maxMessages int) ([]Message, error)
	DeleteMessage(ctx context.Context, receiptHandle string) error
}

// CTUploader is the interface for CleverTap upload operations.
type CTUploader interface {
	Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error)
}

// Consumer polls SQS, transforms events, and uploads to CleverTap.
type Consumer struct {
	sqs       SQSClient
	ct        CTUploader
	cfg       *transform.Config
	batchSize int
	dedup     *lru.Cache[string, struct{}]
}

// NewConsumer creates a Consumer with the given dependencies.
func NewConsumer(sqs SQSClient, ct CTUploader, cfg *transform.Config, batchSize, dedupSize int) *Consumer {
	cache, _ := lru.New[string, struct{}](dedupSize)
	return &Consumer{
		sqs:       sqs,
		ct:        ct,
		cfg:       cfg,
		batchSize: batchSize,
		dedup:     cache,
	}
}

// Run starts the polling loop. Stops when ctx is cancelled. Calls onLoop after each iteration.
func (c *Consumer) Run(ctx context.Context, onLoop func()) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down consumer")
			return
		default:
		}

		processed, err := c.ProcessBatch(ctx)
		if err != nil {
			slog.Error("batch processing failed", "error", err)
		}

		if onLoop != nil {
			onLoop()
		}

		// If no messages were processed, back off to avoid busy-looping
		if processed == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}
}

// ProcessBatch receives messages from SQS, transforms them, and uploads to CleverTap.
// Returns the number of events successfully processed.
func (c *Consumer) ProcessBatch(ctx context.Context) (int, error) {
	msgs, err := c.sqs.ReceiveMessages(ctx, c.batchSize)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	var records []clevertap.EventRecord
	var processedMsgs []Message

	for _, msg := range msgs {
		var evt adapty.Event
		if err := json.Unmarshal([]byte(msg.Body), &evt); err != nil {
			slog.Error("malformed message", "message_id", msg.MessageID, "error", err)
			// Don't delete — let it go to DLQ after max receives
			continue
		}

		// Skip empty event_type (verification requests)
		if evt.EventType == "" {
			slog.Debug("skipping message with empty event_type", "message_id", msg.MessageID)
			_ = c.sqs.DeleteMessage(ctx, msg.ReceiptHandle)
			continue
		}

		// Deduplication
		eventID := getEventID(evt)
		if eventID != "" {
			if _, ok := c.dedup.Get(eventID); ok {
				slog.Debug("skipping duplicate", "profile_event_id", eventID)
				_ = c.sqs.DeleteMessage(ctx, msg.ReceiptHandle)
				continue
			}
		}

		rec, err := transform.Transform(evt, c.cfg)
		if err != nil {
			slog.Error("transform failed", "event_type", evt.EventType, "error", err)
			continue
		}

		records = append(records, rec)
		processedMsgs = append(processedMsgs, msg)

		if eventID != "" {
			c.dedup.Add(eventID, struct{}{})
		}
	}

	if len(records) == 0 {
		return 0, nil
	}

	start := time.Now()
	resp, err := c.ct.Upload(clevertap.UploadRequest{D: records})
	latency := time.Since(start)

	if err != nil {
		// Check for auth errors
		if _, ok := err.(*clevertap.AuthError); ok {
			slog.Error("clevertap auth failure — stopping", "error", err)
			return 0, err
		}
		slog.Error("clevertap upload failed", "error", err, "latency_ms", latency.Milliseconds())
		return 0, err
	}

	// Build set of failed record indices
	failedIndices := make(map[int]bool, len(resp.Unprocessed))
	for _, u := range resp.Unprocessed {
		failedIndices[u.Record] = true
		slog.Error("unprocessed record",
			"record_index", u.Record,
			"error", u.Error,
			"code", u.Code,
		)
	}

	// Delete only successfully processed messages; leave failed ones for SQS redelivery → DLQ
	for i, msg := range processedMsgs {
		evt := records[i]
		if failedIndices[i] {
			slog.Warn("leaving failed message for redelivery",
				"event_type", evt.EvtName,
				"identity", evt.Identity,
				"message_id", msg.MessageID,
			)
			continue
		}
		slog.Info("event processed",
			"event_type", evt.EvtName,
			"identity", evt.Identity,
			"profile_event_id", evt.EvtData["profile_event_id"],
			"status", resp.Status,
			"latency_ms", latency.Milliseconds(),
		)
		_ = c.sqs.DeleteMessage(ctx, msg.ReceiptHandle)
	}

	return len(records) - len(resp.Unprocessed), nil
}

func getEventID(evt adapty.Event) string {
	if evt.EventProperties == nil {
		return ""
	}
	if id, ok := evt.EventProperties["profile_event_id"]; ok {
		if s, ok := id.(string); ok {
			return s
		}
	}
	return ""
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./internal/queue/... -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/queue/consumer.go internal/queue/consumer_test.go go.mod go.sum
git commit -m "feat: add SQS consumer with batching, dedup, and graceful shutdown"
```

---

## Task 6: Connector Entrypoint

**Files:**
- Create: `cmd/connector/main.go`

- [ ] **Step 1: Create SQS adapter**

We need an adapter that wraps the AWS SDK's SQS client to implement our `SQSClient` interface. Add this to `internal/queue/sqs_adapter.go`:

```go
package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSAdapter wraps the AWS SDK SQS client to implement SQSClient.
type SQSAdapter struct {
	client   *sqs.Client
	queueURL string
}

// NewSQSAdapter creates a new SQS adapter.
func NewSQSAdapter(client *sqs.Client, queueURL string) *SQSAdapter {
	return &SQSAdapter{client: client, queueURL: queueURL}
}

// ReceiveMessages long-polls SQS for messages. Caps at 10 per call (AWS limit).
func (a *SQSAdapter) ReceiveMessages(ctx context.Context, maxMessages int) ([]Message, error) {
	if maxMessages > 10 {
		maxMessages = 10 // AWS SQS hard limit per ReceiveMessage call
	}
	out, err := a.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &a.queueURL,
		MaxNumberOfMessages: int32(maxMessages),
		WaitTimeSeconds:     20, // long poll
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]Message, len(out.Messages))
	for i, m := range out.Messages {
		msgs[i] = Message{
			Body:          aws.ToString(m.Body),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			MessageID:     aws.ToString(m.MessageId),
		}
	}
	return msgs, nil
}

// DeleteMessage removes a processed message from SQS.
func (a *SQSAdapter) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := a.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &a.queueURL,
		ReceiptHandle: &receiptHandle,
	})
	return err
}
```

- [ ] **Step 2: Create connector main.go**

Create `cmd/connector/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
	"github.com/anthropic/adapty-ct-connector/internal/queue"
	"github.com/anthropic/adapty-ct-connector/internal/transform"
)

func main() {
	// Configure structured logging
	level := slog.LevelInfo
	if l := os.Getenv("LOG_LEVEL"); l != "" {
		switch l {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	// Load required env vars
	ctAccountID := requireEnv("CT_ACCOUNT_ID")
	ctPasscode := requireEnv("CT_PASSCODE")
	ctRegion := requireEnv("CT_REGION")
	sqsQueueURL := requireEnv("SQS_QUEUE_URL")

	batchSize := envInt("BATCH_SIZE", 10)
	dedupSize := envInt("DEDUP_LRU_SIZE", 100000)
	dryRun := os.Getenv("DRY_RUN") == "true"
	configPath := os.Getenv("TRANSFORM_CONFIG_PATH")

	// Load transform config
	cfg, err := transform.LoadConfig(configPath)
	if err != nil {
		slog.Error("failed to load transform config", "path", configPath, "error", err)
		os.Exit(1)
	}

	// Create CleverTap client
	var ctClient queue.CTUploader
	if dryRun {
		slog.Info("DRY RUN mode — events will be logged but not sent to CleverTap")
		ctClient = &dryRunUploader{}
	} else if baseURL := os.Getenv("CT_BASE_URL"); baseURL != "" {
		// Override for local dev (e.g., mock CleverTap server)
		ctClient = clevertap.NewClient(ctAccountID, ctPasscode, baseURL)
	} else {
		ctClient = clevertap.NewClientFromRegion(ctAccountID, ctPasscode, ctRegion)
	}

	// Create SQS client
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Allow overriding SQS endpoint for LocalStack
	var sqsOpts []func(*sqs.Options)
	if endpoint := os.Getenv("SQS_ENDPOINT"); endpoint != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = &endpoint
		})
	}

	sqsClient := sqs.NewFromConfig(awsCfg, sqsOpts...)
	sqsAdapter := queue.NewSQSAdapter(sqsClient, sqsQueueURL)

	// Create consumer
	consumer := queue.NewConsumer(sqsAdapter, ctClient, cfg, batchSize, dedupSize)

	// Health check server — tracks last successful SQS poll
	var lastSuccess atomic.Int64
	lastSuccess.Store(time.Now().Unix())
	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			elapsed := time.Now().Unix() - lastSuccess.Load()
			if elapsed < 60 {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "unhealthy: no successful poll for %ds", elapsed)
			}
		})
		slog.Info("health check server starting", "port", 8080)
		http.ListenAndServe(":8080", nil)
	}()

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	slog.Info("connector starting",
		"ct_account", ctAccountID,
		"ct_region", ctRegion,
		"sqs_queue", sqsQueueURL,
		"batch_size", batchSize,
		"dry_run", dryRun,
	)

	consumer.Run(ctx, func() { lastSuccess.Store(time.Now().Unix()) })
	slog.Info("connector stopped")
}

type dryRunUploader struct{}

func (d *dryRunUploader) Upload(req clevertap.UploadRequest) (*clevertap.UploadResponse, error) {
	slog.Info("DRY RUN upload", "event_count", len(req.D))
	return &clevertap.UploadResponse{Status: "success", Processed: len(req.D)}, nil
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd /Users/dk/projects/adapty-ct-connector
go mod tidy && go build ./cmd/connector/...
```

Expected: Compiles without errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/connector/main.go internal/queue/sqs_adapter.go go.mod go.sum
git commit -m "feat: add connector entrypoint with health check and graceful shutdown"
```

---

## Task 7: Backfill CLI

**Files:**
- Create: `cmd/backfill/main.go`
- Create: `cmd/backfill/main_test.go`

- [ ] **Step 1: Write backfill test**

Create `cmd/backfill/main_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadNDJSON(t *testing.T) {
	content := `{"profile_id":"p1","customer_user_id":"u1","event_type":"subscription_started","event_datetime":"2026-03-23T10:00:00.000000+0000","event_properties":{"profile_event_id":"e1"}}
{"profile_id":"p2","customer_user_id":"u2","event_type":"trial_started","event_datetime":"2026-03-23T11:00:00.000000+0000","event_properties":{"profile_event_id":"e2"}}
`
	path := filepath.Join(t.TempDir(), "test.ndjson")
	os.WriteFile(path, []byte(content), 0644)

	events, err := readNDJSON(path, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
	if events[0].EventType != "subscription_started" {
		t.Errorf("expected subscription_started, got %s", events[0].EventType)
	}
}

func TestReadNDJSON_WithOffset(t *testing.T) {
	content := `{"profile_id":"p1","event_type":"a","event_datetime":"2026-03-23T10:00:00.000000+0000"}
{"profile_id":"p2","event_type":"b","event_datetime":"2026-03-23T10:00:00.000000+0000"}
{"profile_id":"p3","event_type":"c","event_datetime":"2026-03-23T10:00:00.000000+0000"}
`
	path := filepath.Join(t.TempDir(), "test.ndjson")
	os.WriteFile(path, []byte(content), 0644)

	events, err := readNDJSON(path, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 event (offset 2), got %d", len(events))
	}
	if events[0].ProfileID != "p3" {
		t.Errorf("expected p3, got %s", events[0].ProfileID)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./cmd/backfill/... -v
```

Expected: FAIL — `readNDJSON` doesn't exist.

- [ ] **Step 3: Implement backfill CLI**

Create `cmd/backfill/main.go`:

```go
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
	inputPath := flag.String("input", "", "Path to NDJSON input file (required)")
	batchSize := flag.Int("batch-size", 500, "Events per CleverTap API call")
	concurrency := flag.Int("concurrency", 5, "Max concurrent uploads (max 15)")
	offset := flag.Int("offset", 0, "Skip first N records")
	dryRun := flag.Bool("dry-run", false, "Log events without uploading")
	configPath := flag.String("transform-config", "", "Path to transform config JSON")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if *ctAccountID == "" || *ctPasscode == "" || *ctRegion == "" || *inputPath == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *concurrency > 15 {
		*concurrency = 15
	}
	if *batchSize > 1000 {
		*batchSize = 1000
	}

	cfg, err := transform.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load transform config", "error", err)
		os.Exit(1)
	}

	events, err := readNDJSON(*inputPath, *offset)
	if err != nil {
		slog.Error("failed to read input", "error", err)
		os.Exit(1)
	}

	slog.Info("backfill starting",
		"total_events", len(events),
		"batch_size", *batchSize,
		"concurrency", *concurrency,
		"offset", *offset,
		"dry_run", *dryRun,
	)

	var client *clevertap.Client
	if !*dryRun {
		client = clevertap.NewClientFromRegion(*ctAccountID, *ctPasscode, *ctRegion)
	}

	// Process in batches
	var processed, failed atomic.Int64
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(events); i += *batchSize {
		end := i + *batchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]

		wg.Add(1)
		sem <- struct{}{}
		go func(batchIdx int, batch []adapty.Event) {
			defer wg.Done()
			defer func() { <-sem }()

			records, errs := transform.TransformBatch(batch, cfg)
			for _, e := range errs {
				slog.Error("transform error", "error", e)
				failed.Add(1)
			}

			if len(records) == 0 {
				return
			}

			if *dryRun {
				slog.Info("DRY RUN batch", "batch", batchIdx, "events", len(records))
				processed.Add(int64(len(records)))
				return
			}

			resp, err := client.Upload(clevertap.UploadRequest{D: records})
			if err != nil {
				slog.Error("upload failed", "batch", batchIdx, "error", err)
				failed.Add(int64(len(records)))
				return
			}

			processed.Add(int64(resp.Processed))
			failed.Add(int64(len(resp.Unprocessed)))

			slog.Info("batch complete",
				"batch", batchIdx,
				"processed", resp.Processed,
				"unprocessed", len(resp.Unprocessed),
				"total_processed", processed.Load(),
			)
		}(i / *batchSize, batch)
	}

	wg.Wait()

	slog.Info("backfill complete",
		"total_processed", processed.Load(),
		"total_failed", failed.Load(),
		"total_input", len(events),
	)
}

func readNDJSON(path string, offset int) ([]adapty.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	var events []adapty.Event
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		if lineNum <= offset {
			continue
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var evt adapty.Event
		if err := json.Unmarshal(line, &evt); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		events = append(events, evt)
	}
	return events, scanner.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/dk/projects/adapty-ct-connector
go test ./cmd/backfill/... -v
```

Expected: All tests PASS.

- [ ] **Step 5: Verify it compiles**

```bash
go build ./cmd/backfill/...
```

Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add cmd/backfill/main.go cmd/backfill/main_test.go
git commit -m "feat: add backfill CLI with NDJSON input, batching, and concurrency"
```

---

## Task 8: Docker & Local Dev Stack

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `cmd/mock-clevertap/main.go`
- Create: `scripts/send-webhook.sh`
- Create: `scripts/seed-queue.sh`

- [ ] **Step 1: Create Dockerfile**

Create `Dockerfile`:

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /connector ./cmd/connector
RUN CGO_ENABLED=0 go build -o /backfill ./cmd/backfill

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /connector /usr/local/bin/connector
COPY --from=builder /backfill /usr/local/bin/backfill
COPY transform-config.json /etc/connector/transform-config.json
ENTRYPOINT ["connector"]
```

- [ ] **Step 2: Create mock CleverTap server**

Create `cmd/mock-clevertap/main.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	http.HandleFunc("/1/upload", func(w http.ResponseWriter, r *http.Request) {
		// Validate headers
		accountID := r.Header.Get("X-CleverTap-Account-Id")
		passcode := r.Header.Get("X-CleverTap-Passcode")
		contentType := r.Header.Get("Content-Type")

		if accountID == "" || passcode == "" {
			slog.Error("missing auth headers", "account_id", accountID)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if contentType != "application/json; charset=utf-8" {
			slog.Warn("unexpected content-type", "content_type", contentType)
		}

		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		var req clevertap.UploadRequest
		if err := json.Unmarshal(body, &req); err != nil {
			slog.Error("invalid JSON", "error", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate each record
		for i, rec := range req.D {
			if rec.Identity == "" {
				slog.Error("missing identity", "record", i)
			}
			if rec.EvtName == "" {
				slog.Error("missing evtName", "record", i)
			}
			if rec.Type != "event" {
				slog.Error("wrong type", "record", i, "type", rec.Type)
			}
			if rec.TS == 0 {
				slog.Warn("missing ts", "record", i)
			}
			slog.Info("received event",
				"record", i,
				"identity", rec.Identity,
				"evtName", rec.EvtName,
				"ts", rec.TS,
				"properties", len(rec.EvtData),
			)
		}

		resp := clevertap.UploadResponse{
			Status:    "success",
			Processed: len(req.D),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	slog.Info("mock CleverTap server starting", "port", port)
	fmt.Println(http.ListenAndServe(":"+port, nil))
}
```

- [ ] **Step 3: Create docker-compose.yml**

Create `docker-compose.yml`:

```yaml
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=sqs
      - DEFAULT_REGION=us-east-1
    volumes:
      - "./scripts/init-localstack.sh:/etc/localstack/init/ready.d/init.sh"

  mock-clevertap:
    build:
      context: .
      dockerfile: Dockerfile.mock
    ports:
      - "8080:8080"

  connector:
    build:
      context: .
    depends_on:
      - localstack
      - mock-clevertap
    environment:
      - CT_ACCOUNT_ID=test-account
      - CT_PASSCODE=test-passcode
      - CT_REGION=us1
      - CT_BASE_URL=http://mock-clevertap:8080/1/upload
      - SQS_QUEUE_URL=http://localstack:4566/000000000000/adapty-ct-test
      - SQS_ENDPOINT=http://localstack:4566
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=us-east-1
      - LOG_LEVEL=debug
      - BATCH_SIZE=10
```

- [ ] **Step 4: Create mock Dockerfile**

Create `Dockerfile.mock`:

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /mock-clevertap ./cmd/mock-clevertap

FROM alpine:3.21
COPY --from=builder /mock-clevertap /usr/local/bin/mock-clevertap
ENTRYPOINT ["mock-clevertap"]
```

- [ ] **Step 5: Create LocalStack init script**

Create `scripts/init-localstack.sh`:

```bash
#!/bin/bash
awslocal sqs create-queue --queue-name adapty-ct-test
awslocal sqs create-queue --queue-name adapty-ct-test-dlq
echo "SQS queues created"
```

Make it executable:

```bash
chmod +x scripts/init-localstack.sh
```

- [ ] **Step 6: Create send-webhook.sh**

Create `scripts/send-webhook.sh`:

```bash
#!/bin/bash
# Sends a single test event to the local SQS queue
set -euo pipefail

QUEUE_URL="${SQS_QUEUE_URL:-http://localhost:4566/000000000000/adapty-ct-test}"
ENDPOINT="${SQS_ENDPOINT:-http://localhost:4566}"
TESTDATA_DIR="$(cd "$(dirname "$0")/../testdata" && pwd)"
PAYLOAD_FILE="${1:-$TESTDATA_DIR/subscription_started.json}"

echo "Sending $(basename "$PAYLOAD_FILE") to SQS..."
aws --endpoint-url="$ENDPOINT" sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "$(cat "$PAYLOAD_FILE")" \
  --region us-east-1

echo "Done."
```

Make it executable:

```bash
chmod +x scripts/send-webhook.sh
```

- [ ] **Step 7: Create seed-queue.sh**

Create `scripts/seed-queue.sh`:

```bash
#!/bin/bash
# Bulk-loads all testdata payloads into the local SQS queue
set -euo pipefail

QUEUE_URL="${SQS_QUEUE_URL:-http://localhost:4566/000000000000/adapty-ct-test}"
ENDPOINT="${SQS_ENDPOINT:-http://localhost:4566}"
TESTDATA_DIR="$(cd "$(dirname "$0")/../testdata" && pwd)"
COUNT="${1:-10}"

echo "Seeding SQS with $COUNT events..."
for i in $(seq 1 "$COUNT"); do
  for f in "$TESTDATA_DIR"/*.json; do
    aws --endpoint-url="$ENDPOINT" sqs send-message \
      --queue-url "$QUEUE_URL" \
      --message-body "$(cat "$f")" \
      --region us-east-1 \
      --no-cli-pager > /dev/null
  done
done

echo "Seeded $((COUNT * $(ls "$TESTDATA_DIR"/*.json | wc -l))) events."
```

Make it executable:

```bash
chmod +x scripts/seed-queue.sh
```

- [ ] **Step 8: Verify Docker build**

```bash
cd /Users/dk/projects/adapty-ct-connector
docker build -t adapty-ct-connector .
docker build -f Dockerfile.mock -t mock-clevertap .
```

Expected: Both images build successfully.

- [ ] **Step 9: Commit**

```bash
git add Dockerfile Dockerfile.mock docker-compose.yml cmd/mock-clevertap/main.go scripts/
git commit -m "feat: add Docker, local dev stack with LocalStack, mock CT, and test scripts"
```

---

## Task 9: Integration Test

**Files:**
- Create: none (uses existing Docker stack)

- [ ] **Step 1: Start the local stack**

```bash
cd /Users/dk/projects/adapty-ct-connector
docker compose up -d
```

Wait for LocalStack to be ready:

```bash
docker compose logs localstack | grep "Ready"
```

- [ ] **Step 2: Send a test event**

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test ./scripts/send-webhook.sh
```

- [ ] **Step 3: Verify connector processed it**

```bash
docker compose logs connector | grep "event processed"
```

Expected: Log line with `"event_type":"Subscription Started"` and `"status":"success"`.

- [ ] **Step 4: Verify mock CleverTap received it**

```bash
docker compose logs mock-clevertap | grep "received event"
```

Expected: Log line with `"evtName":"Subscription Started"`, `"identity":"user_42"`, and a non-zero `ts`.

- [ ] **Step 5: Stop the stack**

```bash
docker compose down
```

- [ ] **Step 6: Commit (if any fixes were needed)**

```bash
git add -A
git commit -m "fix: integration test adjustments"
```

---

## Task 10: Infra Team Architecture Document

**Files:**
- Create: `docs/architecture.md`

- [ ] **Step 1: Write the architecture document**

Create `docs/architecture.md` — this is the handoff document for the infrastructure team. It should contain:

1. Architecture diagram (ASCII, same as in the spec)
2. API Gateway setup instructions:
   - Create REST API with resource `/ingest/{ct_account_id}`
   - Add Authorization header validation (API key or Lambda authorizer)
   - Configure AWS service integration to SQS (direct, no Lambda/compute)
   - Map `ct_account_id` path param to select the target SQS queue
   - Return 200 for empty/verification payloads from Adapty
3. Per-account resource provisioning:
   - SQS queue naming: `adapty-ct-{account_id}`
   - DLQ naming: `adapty-ct-{account_id}-dlq`
   - DLQ redrive policy: `maxReceiveCount: 5`
   - Queue retention: 14 days
   - Fargate task definition with env vars (reference the table from the spec)
4. Fargate task configuration:
   - Image: ECR path to the connector image
   - CPU: 256 (0.25 vCPU) — sufficient for sequential processing
   - Memory: 512 MB
   - Health check: HTTP GET `/healthz` on port 8080
   - Desired count: 1 per account
5. CloudWatch alarms:
   - DLQ `ApproximateNumberOfMessagesVisible` > 0
   - Consumer error rate (metric filter on `"level":"error"`)
   - Consumer health check failures
6. Adapty webhook setup:
   - Dashboard: Integrations → Webhooks
   - Endpoint URL: `https://{api-gateway-domain}/ingest/{ct_account_id}`
   - Authorization header: configured value (store in SSM)
   - Enable all event types
   - Enable "Send Attribution", "Send User Attributes", "Send Play Store purchase token"
7. Scaling notes:
   - One Fargate task per account handles ~50M events/month easily
   - SQS has unlimited throughput for standard queues
   - Scale by adding more Fargate tasks (not by increasing task size)
   - For accounts with > 100M events/month, consider increasing `BATCH_SIZE`

- [ ] **Step 2: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: add infra team architecture handoff document"
```

---

## Task 11: Final Cleanup

**Files:**
- Modify: `go.mod` (tidy)
- Create: `.gitignore`

- [ ] **Step 1: Create .gitignore**

Create `.gitignore`:

```
# Binaries
connector
backfill
mock-clevertap
*.exe

# Go
vendor/

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store

# Docker
docker-compose.override.yml
```

- [ ] **Step 2: Tidy dependencies**

```bash
cd /Users/dk/projects/adapty-ct-connector
go mod tidy
```

- [ ] **Step 3: Run all tests**

```bash
go test ./... -v
```

Expected: All tests PASS.

- [ ] **Step 4: Build all binaries**

```bash
go build ./cmd/connector && go build ./cmd/backfill && go build ./cmd/mock-clevertap
```

Expected: All compile without errors.

- [ ] **Step 5: Clean up binaries**

```bash
rm -f connector backfill mock-clevertap
```

- [ ] **Step 6: Commit**

```bash
git add .gitignore go.mod go.sum
git commit -m "chore: add .gitignore and tidy dependencies"
```
