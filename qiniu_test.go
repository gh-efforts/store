package store

import (
	"os"
	"testing"

	logger "github.com/ipfs/go-log/v2"
)

func TestQiniuStore(t *testing.T) {
	logger.SetDebugLogging()
	testPath := "qiniu:/tmp-pqTRKflmcS/.store-test"

	t.Run("QiniuStore1", func(t *testing.T) {
		s, err := NewStore("", "")
		if err != nil {
			t.Fatal(err)
		}
		testAll(t, s, testPath)
	})

	t.Run("QiniuStore2", func(t *testing.T) {
		s, err := NewQiniuStore(os.Getenv(qiniuEnv))
		if err != nil {
			t.Fatal(err)
		}
		_, tp, _ := GetPathProtocol(testPath)
		testAll(t, s, tp)
	})
}
