package store

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupS3Store(t *testing.T) *S3Store {
	cfg := &S3Config{
		Endpoint:  "localhost:9000",
		Region:    "us-east-1",
		Bucket:    "test-bucket",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		UseSSL:    false,
	}
	store, err := NewS3Store(cfg)
	assert.NoError(t, err, "failed to create S3Store")
	return store.(*S3Store)
}

func TestS3Store_UploadData(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-upload-data.txt"

	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3Store_Upload(t *testing.T) {
	store := setupS3Store(t)
	file := "test-upload.txt"
	key := "test-upload.txt"
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

func TestS3Store_UploadReader(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	reader := bytes.NewReader(data)
	key := "test-upload-reader.txt"

	err := store.UploadReader(reader, int64(len(data)), key)
	assert.NoError(t, err, "failed to upload reader")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}

func TestS3Store_Delete(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-delete.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	err = store.Delete(key)
	assert.NoError(t, err, "failed to delete key")
}

func TestS3Store_Exists(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-exists.txt"
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

func TestS3Store_DownloadBytes(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-download-bytes.txt"
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

func TestS3Store_DownloadReader(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-download-reader.txt"
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

func TestS3Store_DownloadRangeBytes(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-download-range-bytes.txt"
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

func TestS3Store_DownloadRangeReader(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-download-range-reader.txt"
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

func TestS3Store_ListPrefix(t *testing.T) {
	store := setupS3Store(t)
	data := []byte("test content")
	key := "test-list-prefix/test-file.txt"
	err := store.UploadData(data, key)
	assert.NoError(t, err, "failed to upload data")

	keys, err := store.ListPrefix("test-list-prefix/")
	assert.NoError(t, err, "failed to list prefix")
	assert.Contains(t, keys, key, "key not found in list")

	defer func() {
		err := store.Delete(key)
		assert.NoError(t, err, "failed to delete key during cleanup")
	}()
}
