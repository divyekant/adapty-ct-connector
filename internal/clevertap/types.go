package clevertap

const (
	RecordTypeEvent = "event"

	StatusSuccess = "success"
	StatusPartial = "partial"
	StatusFail    = "fail"
)

type UploadRequest struct {
	D []EventRecord `json:"d"`
}

type EventRecord struct {
	Identity string                 `json:"identity"`
	TS       int64                  `json:"ts"`
	Type     string                 `json:"type"`
	EvtName  string                 `json:"evtName"`
	EvtData  map[string]interface{} `json:"evtData"`
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
