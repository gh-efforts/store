package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOSStore_ListPrefix(t *testing.T) {
	store := NewOSStore()
	dir := t.TempDir()
	file1 := filepath.Join(dir, "file1.txt")
	file2 := filepath.Join(dir, "file2.txt")
	_ = os.WriteFile(file1, []byte("content1"), 0644)
	_ = os.WriteFile(file2, []byte("content2"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	keys, err := store.ListPrefix(dir)
	assert.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestOSStore_Stat(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	_ = os.WriteFile(file, []byte("content"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	stat, err := store.Stat(file)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), stat.Size)
}

func TestOSStore_UploadData(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	err := store.UploadData(data, file)
	assert.NoError(t, err)

	content, err := os.ReadFile(file)
	assert.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestOSStore_Upload(t *testing.T) {
	store := NewOSStore()
	srcFile := filepath.Join(t.TempDir(), "src.txt")
	destFile := filepath.Join(t.TempDir(), "dest.txt")
	_ = os.WriteFile(srcFile, []byte("content"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(srcFile))
		_ = os.RemoveAll(filepath.Dir(destFile))
	})

	err := store.Upload(srcFile, destFile)
	assert.NoError(t, err)

	content, err := os.ReadFile(destFile)
	assert.NoError(t, err)
	assert.Equal(t, "content", string(content))
}

func TestOSStore_UploadReader(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")
	reader := bytes.NewReader(data)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	err := store.UploadReader(reader, int64(len(data)), file)
	assert.NoError(t, err)

	content, err := os.ReadFile(file)
	assert.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestOSStore_DeleteDirectory(t *testing.T) {
	store := NewOSStore()
	dir := t.TempDir()
	file := filepath.Join(dir, "file.txt")
	_ = os.WriteFile(file, []byte("content"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	err := store.DeleteDirectory(dir)
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestOSStore_Delete(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	_ = os.WriteFile(file, []byte("content"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	err := store.Delete(file)
	assert.NoError(t, err)

	_, err = os.Stat(file)
	assert.True(t, os.IsNotExist(err))
}

func TestOSStore_Exists(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	_ = os.WriteFile(file, []byte("content"), 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	exists, err := store.Exists(file)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestOSStore_DownloadBytes(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")
	_ = os.WriteFile(file, data, 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	content, err := store.DownloadBytes(file)
	assert.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestOSStore_DownloadReader(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")
	_ = os.WriteFile(file, data, 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	reader, err := store.DownloadReader(file)
	assert.NoError(t, err)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	content, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestOSStore_DownloadRangeBytes(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")
	_ = os.WriteFile(file, data, 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	content, err := store.DownloadRangeBytes(file, 0, 4)
	assert.NoError(t, err)
	assert.Equal(t, data[:4], content)
}

func TestOSStore_DownloadRangeReader(t *testing.T) {
	store := NewOSStore()
	file := filepath.Join(t.TempDir(), "file.txt")
	data := []byte("content")
	_ = os.WriteFile(file, data, 0644)

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(file))
	})

	reader, err := store.DownloadRangeReader(file, 0, 4)
	assert.NoError(t, err)
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	content, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, data[:4], content)
}
