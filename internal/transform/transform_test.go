package transform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
)

// loadTestEvent reads an Adapty event from a testdata JSON file relative to the repo root.
func loadTestEvent(t *testing.T, filename string) adapty.Event {
	t.Helper()
	// Navigate from internal/transform up two levels to reach the repo root testdata dir.
	_, callerFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(callerFile), "..", "..")
	path := filepath.Join(repoRoot, "testdata", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("loadTestEvent: failed to read %s: %v", path, err)
	}
	var event adapty.Event
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("loadTestEvent: failed to unmarshal %s: %v", filename, err)
	}
	return event
}

func TestIdentityFromCustomerUserID(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Identity != "user-abc" {
		t.Errorf("expected identity %q, got %q", "user-abc", record.Identity)
	}
}

func TestIdentityFallbackToProfileID(t *testing.T) {
	event := loadTestEvent(t, "access_level_updated.json")
	cfg := DefaultConfig()
	// customer_user_id is null in this fixture → should fall back to profile_id
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Identity != "prof-222" {
		t.Errorf("expected identity %q, got %q", "prof-222", record.Identity)
	}
}

func TestKnownEventNameMapping(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.EvtName != "Subscription Started" {
		t.Errorf("expected event name %q, got %q", "Subscription Started", record.EvtName)
	}
}

func TestUnknownEventNameFallback(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	event.EventType = "some_new_adapty_event"
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "Adapty some_new_adapty_event"
	if record.EvtName != expected {
		t.Errorf("expected event name %q, got %q", expected, record.EvtName)
	}
}

func TestTimestampConversion(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	// event_datetime = "2026-03-23T10:00:00.000000+0000" → 1774436400
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const expected int64 = 1774260000
	if record.TS != expected {
		t.Errorf("expected ts %d, got %d", expected, record.TS)
	}
}

func TestNullFieldOmission(t *testing.T) {
	event := loadTestEvent(t, "access_level_updated.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// promotional_offer_id is null in event_properties → must not appear in evtData
	if _, ok := record.EvtData["promotional_offer_id"]; ok {
		t.Error("expected promotional_offer_id to be omitted (null), but it was present")
	}
	// idfv, idfa etc. are null in top_level → must not appear
	for _, field := range []string{"idfv", "idfa", "advertising_id", "user_agent", "email"} {
		if _, ok := record.EvtData[field]; ok {
			t.Errorf("expected null field %q to be omitted, but it was present", field)
		}
	}
}

func TestLayer2OverridesLayer1(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// event_properties.profile_id = "OVERRIDDEN_BY_LAYER2" should win over top-level profile_id "prof-111"
	val, ok := record.EvtData["profile_id"]
	if !ok {
		t.Fatal("expected profile_id in evtData")
	}
	if val != "OVERRIDDEN_BY_LAYER2" {
		t.Errorf("expected profile_id %q (layer 2 override), got %q", "OVERRIDDEN_BY_LAYER2", val)
	}
}

func TestAttributionFlattening(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := map[string]string{
		"attribution_appsflyer_campaign":       "campaign_abc",
		"attribution_appsflyer_channel":        "email",
		"attribution_appsflyer_ad_set":         "summer_sale",
		"attribution_appsflyer_network_user_id": "af-net-user-1",
	}
	for key, want := range checks {
		got, ok := record.EvtData[key]
		if !ok {
			t.Errorf("expected key %q in evtData, not found", key)
			continue
		}
		if got != want {
			t.Errorf("key %q: expected %q, got %v", key, want, got)
		}
	}
}

func TestAttributionNullFieldsOmitted(t *testing.T) {
	event := loadTestEvent(t, "full_payload.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// adjust.ad_set and adjust.ad_group are null in full_payload.json
	for _, key := range []string{"attribution_adjust_ad_set", "attribution_adjust_ad_group", "attribution_adjust_creative"} {
		if _, ok := record.EvtData[key]; ok {
			t.Errorf("expected null attribution field %q to be omitted", key)
		}
	}
	// but non-null adjust fields should be present
	if _, ok := record.EvtData["attribution_adjust_campaign"]; !ok {
		t.Error("expected attribution_adjust_campaign to be present")
	}
}

func TestUserAttributeFlattening(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := map[string]interface{}{
		"user_attr_age":  float64(30), // JSON numbers decode as float64
		"user_attr_plan": "annual",
	}
	for key, want := range checks {
		got, ok := record.EvtData[key]
		if !ok {
			t.Errorf("expected key %q in evtData, not found", key)
			continue
		}
		if got != want {
			t.Errorf("key %q: expected %v, got %v", key, want, got)
		}
	}
}

func TestIntegrationIDFlattening(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := map[string]string{
		"integration_firebase_app_instance_id": "firebase-id-123",
		"integration_branch_id":                "branch-id-456",
	}
	for key, want := range checks {
		got, ok := record.EvtData[key]
		if !ok {
			t.Errorf("expected key %q in evtData, not found", key)
			continue
		}
		if got != want {
			t.Errorf("key %q: expected %q, got %v", key, want, got)
		}
	}
}

func TestPlayStorePurchaseTokenFlattening(t *testing.T) {
	event := loadTestEvent(t, "full_payload.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := map[string]interface{}{
		"play_store_product_id":      "com.example.annual",
		"play_store_purchase_token":  "tok_abc123xyz",
		"play_store_is_subscription": true,
	}
	for key, want := range checks {
		got, ok := record.EvtData[key]
		if !ok {
			t.Errorf("expected key %q in evtData, not found", key)
			continue
		}
		if got != want {
			t.Errorf("key %q: expected %v, got %v", key, want, got)
		}
	}
}

func TestPlayStoreAbsentWhenNil(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, key := range []string{"play_store_product_id", "play_store_purchase_token", "play_store_is_subscription"} {
		if _, ok := record.EvtData[key]; ok {
			t.Errorf("play_store field %q should be absent when play_store_purchase_token is null", key)
		}
	}
}

func TestProfilesSharingSerialization(t *testing.T) {
	event := loadTestEvent(t, "full_payload.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw, ok := record.EvtData["profiles_sharing_access_level"]
	if !ok {
		t.Fatal("expected profiles_sharing_access_level in evtData")
	}
	s, ok := raw.(string)
	if !ok {
		t.Fatalf("expected profiles_sharing_access_level to be a string, got %T", raw)
	}
	// Must be valid JSON array
	var arr []map[string]interface{}
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		t.Fatalf("profiles_sharing_access_level is not valid JSON: %v — value: %s", err, s)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 profiles in sharing array, got %d", len(arr))
	}
	if arr[0]["profile_id"] != "prof-child-1" {
		t.Errorf("expected first profile_id %q, got %v", "prof-child-1", arr[0]["profile_id"])
	}
}

func TestProfilesSharingAbsentWhenEmpty(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := record.EvtData["profiles_sharing_access_level"]; ok {
		t.Error("profiles_sharing_access_level should be absent when array is null/empty")
	}
}

func TestDisabledLayerExclusion(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := &Config{
		DisabledLayers: []string{"attributions", "user_attributes"},
	}
	cfg.buildLookups()

	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// attribution keys must be absent
	if _, ok := record.EvtData["attribution_appsflyer_campaign"]; ok {
		t.Error("attribution_appsflyer_campaign should be absent when attributions layer is disabled")
	}
	// user_attr keys must be absent
	if _, ok := record.EvtData["user_attr_age"]; ok {
		t.Error("user_attr_age should be absent when user_attributes layer is disabled")
	}
	// top_level keys should still be present (layer not disabled)
	if _, ok := record.EvtData["idfv"]; !ok {
		t.Error("idfv should be present (top_level layer is enabled)")
	}
}

func TestExcludedFieldFiltering(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := &Config{
		ExcludedFields: map[string][]string{
			"top_level": {"email", "user_agent"},
		},
	}
	cfg.buildLookups()

	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := record.EvtData["email"]; ok {
		t.Error("email should be excluded from evtData")
	}
	if _, ok := record.EvtData["user_agent"]; ok {
		t.Error("user_agent should be excluded from evtData")
	}
	// Other top_level fields should still appear
	if _, ok := record.EvtData["idfv"]; !ok {
		t.Error("idfv should still be in evtData (not excluded)")
	}
}

func TestEmptyEventTypeError(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	event.EventType = ""
	cfg := DefaultConfig()
	_, err := Transform(event, cfg)
	if err == nil {
		t.Error("expected an error for empty event_type, got nil")
	}
}

func TestTypeFieldIsEvent(t *testing.T) {
	event := loadTestEvent(t, "subscription_started.json")
	cfg := DefaultConfig()
	record, err := Transform(event, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Type != "event" {
		t.Errorf("expected Type %q, got %q", "event", record.Type)
	}
}

func TestTransformBatch(t *testing.T) {
	e1 := loadTestEvent(t, "subscription_started.json")
	e2 := loadTestEvent(t, "access_level_updated.json")
	cfg := DefaultConfig()

	records, errs := TransformBatch([]adapty.Event{e1, e2}, cfg)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors in TransformBatch: %v", errs)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Identity != "user-abc" {
		t.Errorf("record[0]: expected identity %q, got %q", "user-abc", records[0].Identity)
	}
	if records[1].Identity != "prof-222" {
		t.Errorf("record[1]: expected identity %q, got %q", "prof-222", records[1].Identity)
	}
}

func TestTransformBatchWithInvalidEvent(t *testing.T) {
	e1 := loadTestEvent(t, "subscription_started.json")
	invalid := adapty.Event{} // empty event_type → error
	cfg := DefaultConfig()

	records, errs := TransformBatch([]adapty.Event{e1, invalid}, cfg)
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
	if len(records) != 1 {
		t.Errorf("expected 1 successful record, got %d", len(records))
	}
}
