package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pelletier/go-toml"
)

type S3MultiStoreConfig struct {
	path         string
	selectConfig func(cfgs map[string]*S3Config, key string) (*S3Config, bool)
	cfgs         map[string]*S3Config
	lk           sync.RWMutex
}

func (s *S3MultiStoreConfig) getStore(key string) (Interface, error) {
	s.lk.RLock()
	defer s.lk.RUnlock()

	cfg, ok := s.selectConfig(s.cfgs, key)
	if !ok {
		return nil, fmt.Errorf("no s3 configuration found for key: %s", key)
	}

	return NewS3Store(cfg)
}

func LoadS3MultiStoreConfig(cfgPath string) (*S3MultiStoreConfig, error) {
	cfgs := make(map[string]*S3Config)

	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("read s3 configuration file error: %v", err)
	}
	ext := strings.ToLower(path.Ext(cfgPath))
	if ext == ".json" {
		err = json.Unmarshal(raw, &cfgs)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &cfgs)
	} else {
		return nil, fmt.Errorf("invalid s3 configuration format")
	}
	if err != nil {
		return nil, fmt.Errorf("unmarshal s3 configuration error: %v", err)
	}
	return &S3MultiStoreConfig{path: cfgPath, cfgs: cfgs, selectConfig: defaultSelectConfigCallbackFunc}, nil
}

func isKeyStartsWithPrefix(key, prefix string) bool {
	if !strings.HasSuffix(key, "/") {
		key = key + "/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return strings.HasPrefix(key, prefix)
}

var defaultSelectConfigCallbackFunc = func(cfgs map[string]*S3Config, key string) (*S3Config, bool) {
	for keyPrefix, config := range cfgs {
		if isKeyStartsWithPrefix(key, keyPrefix) {
			return config, true
		}
	}
	return nil, false
}
