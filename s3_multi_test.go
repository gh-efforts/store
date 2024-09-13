package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupS3MultiStore(t *testing.T) *S3MultiStore {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgContent := `{
        "prefix1": {
            "endpoint": "localhost:9000",
            "region": "us-east-1",
            "bucket": "test-bucket",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "use_ssl": false
        }
    }`
	err := os.WriteFile(cfgPath, []byte(cfgContent), 0644)
	assert.NoError(t, err, "failed to write config file")

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(cfgPath))
	})

	store, err := NewS3MultiStore(cfgPath)
	assert.NoError(t, err, "failed to create S3MultiStore")
	return store.(*S3MultiStore)
}

func TestS3MultiStore_UploadData(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-upload-data.txt"

	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_Upload(t *testing.T) {
	store := setupS3MultiStore(t)
	file := "test-upload.txt"
	key := "prefix1/test-upload.txt"
	err := os.WriteFile(file, []byte("test content"), 0644)
	assert.NoError(t, err, "failed to write test file")

	err = store.Upload(file, key)
	assert.NoError(t, err, "failed to upload file")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
		err = os.Remove(file)
		assert.NoError(t, err, "failed to remove file during cleanup")
	}()
}

func TestS3MultiStore_UploadReader(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	reader := bytes.NewReader(data)
	key := "prefix1/test-upload-reader.txt"

	err := store.UploadReader(reader, int64(len(data)), key)
	assert.NoError(t, err, "failed to upload reader")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_Delete(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-delete.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	err = store.Delete(key)
	assert.NoError(t, err, "failed to delete key")
}

func TestS3MultiStore_Exists(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-exists.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	exists, err := store.Exists(key)
	assert.NoError(t, err, "failed to check existence")
	assert.True(t, exists, "key should exist")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_DownloadBytes(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-download-bytes.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	downloadedData, err := store.DownloadBytes(key)
	assert.NoError(t, err, "failed to download bytes")
	assert.Equal(t, data, downloadedData, "downloaded data mismatch")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_DownloadReader(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-download-reader.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	reader, err := store.DownloadReader(key)
	assert.NoError(t, err, "failed to download reader")
	downloadedData, err := io.ReadAll(reader)
	assert.NoError(t, err, "failed to read from reader")
	assert.Equal(t, data, downloadedData, "downloaded data mismatch")
	_ = reader.Close()

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_DownloadRangeBytes(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-download-range-bytes.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	downloadedData, err := store.DownloadRangeBytes(key, 0, 4)
	assert.NoError(t, err, "failed to download range bytes")
	assert.Equal(t, data[:4], downloadedData, "downloaded data mismatch")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_DownloadRangeReader(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-download-range-reader.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	reader, err := store.DownloadRangeReader(key, 0, 4)
	assert.NoError(t, err, "failed to download range reader")
	downloadedData, err := io.ReadAll(reader)
	assert.NoError(t, err, "failed to read from reader")
	assert.Equal(t, data[:4], downloadedData, "downloaded data mismatch")
	_ = reader.Close()

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3MultiStore_ListPrefix(t *testing.T) {
	store := setupS3MultiStore(t)
	data := []byte("test content")
	key := "prefix1/test-list-prefix/test-file.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	keys, err := store.ListPrefix("prefix1/test-list-prefix/")
	assert.NoError(t, err, "failed to list prefix")
	assert.Contains(t, keys, key, "key not found in list")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}