package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadS3MultiStoreConfig_JSON(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgContent := `{
        "prefix1": {
            "endpoint": "localhost:9000",
            "region": "us-east-1",
            "bucket": "bucket1",
            "accessKey": "key1",
            "secretKey": "secret1",
            "useSSL": false
        }
    }`
	err := os.WriteFile(cfgPath, []byte(cfgContent), 0644)
	assert.NoError(t, err, "failed to write config file")

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(cfgPath))
	})

	cfg, err := LoadS3MultiStoreConfig(cfgPath)
	assert.NoError(t, err, "failed to load config")
	assert.NotNil(t, cfg, "config should not be nil")
	assert.Equal(t, "localhost:9000", cfg.cfgs["prefix1"].Endpoint, "unexpected endpoint")
}

func TestLoadS3MultiStoreConfig_TOML(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	cfgContent := `
[prefix1]
endpoint = "localhost:9000"
region = "us-east-1"
bucket = "bucket1"
accessKey = "key1"
secretKey = "secret1"
useSSL = false
`
	err := os.WriteFile(cfgPath, []byte(cfgContent), 0644)
	assert.NoError(t, err, "failed to write config file")

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(cfgPath))
	})

	cfg, err := LoadS3MultiStoreConfig(cfgPath)
	assert.NoError(t, err, "failed to load config")
	assert.NotNil(t, cfg, "config should not be nil")
	assert.Equal(t, "localhost:9000", cfg.cfgs["prefix1"].Endpoint, "unexpected endpoint")
}

func TestS3MultiStoreConfig_getStore(t *testing.T) {
	cfg := &S3MultiStoreConfig{
		cfgs: map[string]*S3Config{
			"prefix1": {
				Endpoint:  "localhost:9000",
				Region:    "us-east-1",
				Bucket:    "bucket1",
				AccessKey: "key1",
				SecretKey: "secret1",
				UseSSL:    false,
			},
		},
		selectConfig: defaultSelectConfigCallbackFunc,
	}

	store, err := cfg.getStore("prefix1/some/key")
	assert.NoError(t, err, "failed to get store")
	assert.NotNil(t, store, "store should not be nil")
}

func TestIsKeyStartsWithPrefix(t *testing.T) {
	assert.True(t, isKeyStartsWithPrefix("prefix1/some/key", "prefix1"), "expected true for matching prefix")
	assert.False(t, isKeyStartsWithPrefix("prefix2/some/key", "prefix1"), "expected false for non-matching prefix")
}
