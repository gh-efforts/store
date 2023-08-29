package store

import (
	"os"
	"testing"

	logger "github.com/ipfs/go-log/v2"
)

func TestS3Store(t *testing.T) {
	logger.SetDebugLogging()
	testPath := "s3:/tmp-pqTRKflmcS/.store-test"

	t.Run("S3Store1", func(t *testing.T) {
		s, err := NewStore("", "")
		if err != nil {
			t.Fatal(err)
		}
		testAll(t, s, testPath)
	})

	t.Run("S3Store2", func(t *testing.T) {
		s, err := NewS3Store(os.Getenv(S3Env))
		if err != nil {
			t.Fatal(err)
		}
		_, tp, _ := GetPathProtocol(testPath)
		testAll(t, s, tp)
	})
}
