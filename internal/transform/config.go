package transform

import (
	"encoding/json"
	"os"
)

// DefaultCleverTapIDAttribute is the Adapty user_attributes key checked for a
// CleverTap ID when clevertap_id_attribute is not set in the config file.
const DefaultCleverTapIDAttribute = "clevertap_id"

// Config controls which layers and fields are included in the transformation.
type Config struct {
	DisabledLayers []string            `json:"disabled_layers"`
	ExcludedFields map[string][]string `json:"excluded_fields"`

	// CleverTapIDAttribute names the user_attributes key that carries the
	// CleverTap ID. When that attribute is present on an event, records are
	// keyed by objectId (the CleverTap ID) instead of identity. nil means
	// DefaultCleverTapIDAttribute; an explicit "" disables promotion.
	CleverTapIDAttribute *string `json:"clevertap_id_attribute"`

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

// cleverTapIDKey returns the user_attributes key holding the CleverTap ID,
// or "" when objectId promotion is disabled.
func (c *Config) cleverTapIDKey() string {
	if c.CleverTapIDAttribute == nil {
		return DefaultCleverTapIDAttribute
	}
	return *c.CleverTapIDAttribute
}

// IsFieldExcluded returns true if the given field in the given layer is excluded.
func (c *Config) IsFieldExcluded(layer, field string) bool {
	if s, ok := c.excludedSet[layer]; ok {
		return s[field]
	}
	return false
}
