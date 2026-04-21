package transform

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthropic/adapty-ct-connector/internal/adapty"
	"github.com/anthropic/adapty-ct-connector/internal/clevertap"
)

// Layer names for use in Config.IsLayerDisabled / IsFieldExcluded.
const (
	LayerTopLevel         = "top_level"
	LayerEventProperties  = "event_properties"
	LayerAttributions     = "attributions"
	LayerUserAttributes   = "user_attributes"
	LayerIntegrationIDs   = "integration_ids"
	LayerPlayStore        = "play_store"
	LayerProfilesSharing  = "profiles_sharing"
)

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
// Returns an error if event_type is empty.
func Transform(event adapty.Event, cfg *Config) (clevertap.EventRecord, error) {
	if event.EventType == "" {
		return clevertap.EventRecord{}, fmt.Errorf("transform: empty event_type")
	}

	evtData := make(map[string]interface{})

	// Layer 1: top-level profile fields
	if !cfg.IsLayerDisabled(LayerTopLevel) {
		addStringField(evtData, cfg, "top_level", "profile_id", event.ProfileID)
		addPtrStringField(evtData, cfg, "top_level", "idfv", event.IDFV)
		addPtrStringField(evtData, cfg, "top_level", "idfa", event.IDFA)
		addPtrStringField(evtData, cfg, "top_level", "advertising_id", event.AdvertisingID)
		addPtrStringField(evtData, cfg, "top_level", "profile_install_datetime", event.ProfileInstallDatetime)
		addPtrStringField(evtData, cfg, "top_level", "user_agent", event.UserAgent)
		addPtrStringField(evtData, cfg, "top_level", "email", event.Email)
		addPtrIntField(evtData, cfg, "top_level", "event_api_version", event.EventAPIVersion)
	}

	// Layer 2: event_properties (overrides Layer 1 on collision)
	if !cfg.IsLayerDisabled(LayerEventProperties) {
		for k, v := range event.EventProperties {
			if cfg.IsFieldExcluded(LayerEventProperties, k) {
				continue
			}
			if v == nil {
				continue
			}
			evtData[k] = v
		}
	}

	// Layer 3: attributions
	if !cfg.IsLayerDisabled(LayerAttributions) {
		for source, attr := range event.Attributions {
			flattenAttribution(evtData, cfg, source, attr)
		}
	}

	// Layer 4: user_attributes
	if !cfg.IsLayerDisabled(LayerUserAttributes) {
		for k, v := range event.UserAttributes {
			if cfg.IsFieldExcluded(LayerUserAttributes, k) {
				continue
			}
			if v == nil {
				continue
			}
			evtData["user_attr_"+k] = v
		}
	}

	// Layer 5: integration_ids
	if !cfg.IsLayerDisabled(LayerIntegrationIDs) {
		for k, v := range event.IntegrationIDs {
			if cfg.IsFieldExcluded(LayerIntegrationIDs, k) {
				continue
			}
			evtData["integration_"+k] = v
		}
	}

	// Layer 6: play_store_purchase_token
	if !cfg.IsLayerDisabled(LayerPlayStore) && event.PlayStorePurchaseToken != nil {
		ps := event.PlayStorePurchaseToken
		addStringField(evtData, cfg, "play_store", "play_store_product_id", ps.ProductID)
		addStringField(evtData, cfg, "play_store", "play_store_purchase_token", ps.PurchaseToken)
		if !cfg.IsFieldExcluded("play_store", "play_store_is_subscription") {
			evtData["play_store_is_subscription"] = ps.IsSubscription
		}
	}

	// Layer 7: profiles_sharing_access_level
	if !cfg.IsLayerDisabled(LayerProfilesSharing) && len(event.ProfilesSharingAccessLevel) > 0 {
		if !cfg.IsFieldExcluded("profiles_sharing", "profiles_sharing_access_level") {
			b, err := json.Marshal(event.ProfilesSharingAccessLevel)
			if err == nil {
				evtData["profiles_sharing_access_level"] = string(b)
			}
		}
	}

	return clevertap.EventRecord{
		Identity: resolveIdentity(event),
		TS:       parseTimestamp(event.EventDatetime),
		Type:     clevertap.RecordTypeEvent,
		EvtName:  resolveEventName(event.EventType),
		EvtData:  evtData,
	}, nil
}

// BuildProfileRecord returns a CleverTap profile record for the given Adapty event
// when there is profile data worth sending (email or non-empty user_attributes).
// Returns (record, true) when a record should be sent; (zero, false) otherwise.
// Standard CT profile fields (Email) are mapped explicitly; user_attributes flow
// through as custom profile properties using their original keys.
func BuildProfileRecord(event adapty.Event, cfg *Config) (clevertap.EventRecord, bool) {
	profileData := make(map[string]interface{})

	if event.Email != nil && *event.Email != "" && !cfg.IsFieldExcluded(LayerTopLevel, "email") {
		profileData["Email"] = *event.Email
	}

	if !cfg.IsLayerDisabled(LayerUserAttributes) {
		for k, v := range event.UserAttributes {
			if cfg.IsFieldExcluded(LayerUserAttributes, k) {
				continue
			}
			if v == nil {
				continue
			}
			profileData[k] = v
		}
	}

	if len(profileData) == 0 {
		return clevertap.EventRecord{}, false
	}

	return clevertap.EventRecord{
		Identity:    resolveIdentity(event),
		TS:          parseTimestamp(event.EventDatetime),
		Type:        clevertap.RecordTypeProfile,
		ProfileData: profileData,
	}, true
}

// TransformBatch converts a slice of Adapty events into CleverTap EventRecords.
// Errors from individual events are collected; successful records are returned alongside any errors.
func TransformBatch(events []adapty.Event, cfg *Config) ([]clevertap.EventRecord, []error) {
	records := make([]clevertap.EventRecord, 0, len(events))
	var errs []error
	for _, e := range events {
		r, err := Transform(e, cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		records = append(records, r)
	}
	return records, errs
}

// resolveIdentity returns customer_user_id if non-nil and non-empty, else profile_id.
func resolveIdentity(event adapty.Event) string {
	if event.CustomerUserID != nil && *event.CustomerUserID != "" {
		return *event.CustomerUserID
	}
	return event.ProfileID
}

// resolveEventName maps a known event type or falls back to "Adapty {type}".
func resolveEventName(eventType string) string {
	if name, ok := eventNameMap[eventType]; ok {
		return name
	}
	return "Adapty " + eventType
}

// parseTimestamp parses ISO 8601 datetime and returns UNIX epoch seconds.
// Returns 0 on parse failure.
func parseTimestamp(s string) int64 {
	// Try the Adapty format first: "2006-01-02T15:04:05.000000-0700"
	t, err := time.Parse("2006-01-02T15:04:05.000000-0700", s)
	if err != nil {
		// Try without microseconds
		t, err = time.Parse("2006-01-02T15:04:05-0700", s)
		if err != nil {
			return 0
		}
	}
	return t.Unix()
}

// addStringField adds a non-empty string to evtData unless the field is excluded.
func addStringField(evtData map[string]interface{}, cfg *Config, layer, key, val string) {
	if cfg.IsFieldExcluded(layer, key) {
		return
	}
	if val == "" {
		return
	}
	evtData[key] = val
}

// addPtrStringField adds a string pointer field if non-nil and non-excluded.
func addPtrStringField(evtData map[string]interface{}, cfg *Config, layer, key string, val *string) {
	if cfg.IsFieldExcluded(layer, key) {
		return
	}
	if val == nil {
		return
	}
	evtData[key] = *val
}

// addPtrIntField adds an int pointer field if non-nil and non-excluded.
func addPtrIntField(evtData map[string]interface{}, cfg *Config, layer, key string, val *int) {
	if cfg.IsFieldExcluded(layer, key) {
		return
	}
	if val == nil {
		return
	}
	evtData[key] = *val
}

// flattenAttribution adds attribution fields with prefix "attribution_{source}_{field}".
func flattenAttribution(evtData map[string]interface{}, cfg *Config, source string, attr adapty.Attribution) {
	prefix := "attribution_" + source + "_"
	addPtrStringAttr(evtData, cfg, prefix, "ad_set", attr.AdSet)
	addPtrStringAttr(evtData, cfg, prefix, "status", attr.Status)
	addPtrStringAttr(evtData, cfg, prefix, "channel", attr.Channel)
	addPtrStringAttr(evtData, cfg, prefix, "ad_group", attr.AdGroup)
	addPtrStringAttr(evtData, cfg, prefix, "campaign", attr.Campaign)
	addPtrStringAttr(evtData, cfg, prefix, "creative", attr.Creative)
	addPtrStringAttr(evtData, cfg, prefix, "created_at", attr.CreatedAt)
	addPtrStringAttr(evtData, cfg, prefix, "network_user_id", attr.NetworkUserID)
}

// addPtrStringAttr adds a named attribution sub-field if non-nil, checking field exclusion.
func addPtrStringAttr(evtData map[string]interface{}, cfg *Config, prefix, field string, val *string) {
	key := prefix + field
	if cfg.IsFieldExcluded(LayerAttributions, key) {
		return
	}
	if val == nil {
		return
	}
	evtData[key] = *val
}
