package store

import (
	"testing"

	logger "github.com/ipfs/go-log/v2"
)

func TestOSStore(t *testing.T) {
	logger.SetDebugLogging()
	testPath := "/tmp/.store-test"

	t.Run("OSStore1", func(t *testing.T) {
		s, err := NewStore("", "")
		if err != nil {
			t.Fatal(err)
		}
		testAll(t, s, testPath)
	})

	t.Run("OSStore2", func(t *testing.T) {
		s := NewOSStore()
		testAll(t, s, testPath)
	})
}
