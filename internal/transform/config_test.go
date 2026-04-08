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
