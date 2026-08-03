package clevertap

import (
	"encoding/json"
	"testing"
)

func TestEventRecordMarshal_IdentityOnly(t *testing.T) {
	rec := EventRecord{Identity: "user-abc", TS: 1, Type: RecordTypeEvent, EvtName: "E"}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]interface{}
	json.Unmarshal(b, &m)
	if m["identity"] != "user-abc" {
		t.Errorf("expected identity in JSON, got %v", m["identity"])
	}
	if _, has := m["objectId"]; has {
		t.Error("objectId must be omitted when empty")
	}
}

func TestEventRecordMarshal_ObjectIDOnly(t *testing.T) {
	rec := EventRecord{ObjectID: "ctid-123", TS: 1, Type: RecordTypeEvent, EvtName: "E"}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]interface{}
	json.Unmarshal(b, &m)
	if m["objectId"] != "ctid-123" {
		t.Errorf("expected objectId in JSON, got %v", m["objectId"])
	}
	if _, has := m["identity"]; has {
		t.Error("identity must be omitted when empty")
	}
}
