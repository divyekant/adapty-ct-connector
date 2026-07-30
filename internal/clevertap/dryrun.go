package clevertap

import (
	"encoding/json"
	"log/slog"
)

// DryRun implements the upload contract without calling CleverTap. Every
// record that would have been uploaded is logged instead, and the response
// reports full success so the rest of the pipeline (dedup, acking) behaves
// exactly as it would live. Enable via DRY_RUN=true on the consumer.
type DryRun struct{}

func NewDryRun() *DryRun { return &DryRun{} }

func (d *DryRun) Upload(req UploadRequest) (*UploadResponse, error) {
	for i, rec := range req.D {
		body, err := json.Marshal(rec)
		if err != nil {
			body = []byte("<unmarshalable record>")
		}
		slog.Info("dry_run: would upload record",
			"record", i,
			"identity", rec.Identity,
			"type", rec.Type,
			"evt_name", rec.EvtName,
			"payload", string(body),
		)
	}
	return &UploadResponse{Status: StatusSuccess, Processed: len(req.D)}, nil
}
