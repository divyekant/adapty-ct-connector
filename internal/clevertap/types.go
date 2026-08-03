package clevertap

const (
	RecordTypeEvent   = "event"
	RecordTypeProfile = "profile"

	StatusSuccess = "success"
	StatusPartial = "partial"
	StatusFail    = "fail"
)

type UploadRequest struct {
	D []EventRecord `json:"d"`
}

type EventRecord struct {
	// Exactly one of Identity or ObjectID is set. ObjectID carries a CleverTap ID
	// and must be sent as objectId — CleverTap resolves identity and objectId as
	// separate identity keys, so a record must never carry both.
	Identity    string                 `json:"identity,omitempty"`
	ObjectID    string                 `json:"objectId,omitempty"`
	TS          int64                  `json:"ts"`
	Type        string                 `json:"type"`
	EvtName     string                 `json:"evtName,omitempty"`
	EvtData     map[string]interface{} `json:"evtData,omitempty"`
	ProfileData map[string]interface{} `json:"profileData,omitempty"`
}

type UploadResponse struct {
	Status      string        `json:"status"`
	Processed   int           `json:"processed"`
	Unprocessed []Unprocessed `json:"unprocessed"`
}

type Unprocessed struct {
	Status  string `json:"status"`
	Code    int    `json:"code"`
	Error   string `json:"error"`
	Record  int    `json:"record"`
}
