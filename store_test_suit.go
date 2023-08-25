package store

import (
	"crypto/rand"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testAll(t *testing.T, s Interface, testPath string) {
	t.Run("Exists", testExists(s, testPath))
	t.Run("Stat", testStat(s, testPath))
	t.Run("UploadData", testUploadData(s, testPath))
	t.Run("Upload", testUpload(s, testPath))
	t.Run("UploadReader", testUploadReader(s, testPath))
	t.Run("DeleteDirectory", testDeleteDirectory(s, testPath))
	t.Run("Delete", testDelete(s, testPath))
	t.Run("DownloadBytes", testDownloadBytes(s, testPath))
	t.Run("DownloadReader", testDownloadReader(s, testPath))
	t.Run("DownloadRangeBytes", testDownloadRangeBytes(s, testPath))
	t.Run("DownloadRangeReader", testDownloadRangeReader(s, testPath))
	t.Run("ListPrefix", testListPrefix(s, testPath))
}

func testExists(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "exists.txt")
		e, err := s.Exists(key)
		assert.Nil(t, err)
		assert.False(t, e)

		// create
		err = s.UploadData([]byte("exists"), key)
		assert.Nil(t, err)

		// check agent
		e, err = s.Exists(key)
		assert.Nil(t, err)
		assert.True(t, e)

		// delete
		err = s.Delete(key)
		assert.Nil(t, err)

		// check agent
		e, err = s.Exists(key)
		assert.Nil(t, err)
		assert.False(t, e)
	}

}

func testUploadData(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "upload-data.txt")
		value := []byte("upload-data")

		err := s.UploadData(value, key)
		assert.Nil(t, err)

		// check
		e, err := s.Exists(key)
		assert.Nil(t, err)
		assert.True(t, e)

		// delete
		err = s.Delete(key)
		assert.Nil(t, err)
	}
}

func testDelete(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "delete.txt")

		// delete
		err := s.Delete(key)
		assert.NotNil(t, err)

		// create
		err = s.UploadData([]byte("delete"), key)
		assert.Nil(t, err)

		// delete
		err = s.Delete(key)
		assert.Nil(t, err)
	}
}

func testStat(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "stat.txt")

		// create
		err := s.UploadData([]byte("stat"), key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)

		st, err := s.Stat(key)
		assert.Nil(t, err)
		assert.Equal(t, int64(4), st.Size)

		// delete
		err = s.Delete(key)
		assert.Nil(t, err)
	}
}

func testUpload(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		data := "src"
		src := path.Join(os.TempDir(), "src.txt")
		err := os.WriteFile(src, []byte(data), 0644)
		if err != nil {
			t.Fatal(err)
		}
		dst := path.Join(testPath, "dst.txt")
		err = s.Upload(src, dst)
		assert.Nil(t, err)

		// check
		e, err := s.Stat(dst)
		assert.Nil(t, err)
		assert.Equal(t, int64(len(data)), e.Size)

		// delete
		err = s.Delete(dst)
		assert.Nil(t, err)
		err = os.Remove(src)
		assert.Nil(t, err)
	}
}

func testUploadReader(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "upload-reader.txt")

		err := s.UploadReader(io.LimitReader(rand.Reader, 1024), 1024, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)

		// check
		e, err := s.Stat(key)
		assert.Nil(t, err)
		assert.Equal(t, int64(1024), e.Size)

		// delete
		err = s.Delete(key)
		assert.Nil(t, err)
	}
}

func testDeleteDirectory(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		dir := path.Join(testPath, "delete-directory")

		// create
		for i := 0; i < 3; i++ {
			err := s.UploadData([]byte(strconv.Itoa(i)), path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Nil(t, err)
		}

		// check
		for i := 0; i < 3; i++ {
			e, err := s.Exists(path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Nil(t, err)
			assert.True(t, e)
		}

		// delete directory
		err := s.DeleteDirectory(dir)
		assert.Nil(t, err)

		// check
		for i := 0; i < 3; i++ {
			e, err := s.Exists(path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Nil(t, err)
			assert.False(t, e)
		}
	}
}

func testDownloadBytes(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "download-bytes.txt")
		value := []byte("download-bytes")

		// create
		err := s.UploadData(value, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)

		defer func() {
			// delete
			err = s.Delete(key)
			assert.Nil(t, err)
		}()

		// check
		data, err := s.DownloadBytes(key)
		assert.Nil(t, err)
		assert.Equal(t, value, data)
	}
}

func testDownloadRangeBytes(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "download-range-bytes.txt")
		value := []byte("download-range-bytes")

		// create
		err := s.UploadData(value, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)
		defer func() {
			// delete
			err = s.Delete(key)
			assert.Nil(t, err)
		}()

		// check
		var (
			start int64 = 9
			size  int64 = 5
		)
		data, err := s.DownloadRangeBytes(key, start, size) // match "range"
		assert.Nil(t, err)
		assert.Equal(t, value[start:start+size], data)
		assert.Equal(t, []byte("range"), data)
	}
}

func testDownloadReader(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "download-reader.txt")
		value := []byte("download-reader")

		// create
		err := s.UploadData(value, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)

		defer func() {
			// delete
			err = s.Delete(key)
			assert.Nil(t, err)
		}()

		// check
		reader, err := s.DownloadReader(key)
		assert.Nil(t, err)

		data, err := io.ReadAll(reader)
		assert.Nil(t, err)
		assert.Equal(t, value, data)
	}
}

func testDownloadRangeReader(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		key := path.Join(testPath, "download-range-reader.txt")
		value := []byte("download-range-reader")

		// create
		err := s.UploadData(value, key)
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, err)
		defer func() {
			// delete
			err = s.Delete(key)
			assert.Nil(t, err)
		}()

		// check
		var (
			start int64 = 9
			size  int64 = 5
		)
		reader, err := s.DownloadRangeReader(key, start, size) // match "range"
		assert.Nil(t, err)

		data, err := io.ReadAll(reader)
		assert.Nil(t, err)
		assert.Equal(t, value[start:start+size], data)
		assert.Equal(t, []byte("range"), data)
	}
}

func testListPrefix(s Interface, testPath string) func(t *testing.T) {
	return func(t *testing.T) {
		dir := path.Join(testPath, "list-prefix")

		// create
		for i := 0; i < 3; i++ {
			err := s.UploadData([]byte(strconv.Itoa(i)), path.Join(dir, "file-"+strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
		}

		// check
		keys, err := s.ListPrefix(dir)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(keys))

		for i := 0; i < 3; i++ {
			_, p, _ := GetPathProtocol(path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Contains(t, keys, strings.TrimPrefix(p, "/"))

			e, err := s.Exists(path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Nil(t, err)
			assert.True(t, e)
		}

		//delete directory
		err = s.DeleteDirectory(dir)
		assert.Nil(t, err)

		// check
		for i := 0; i < 3; i++ {
			e, err := s.Exists(path.Join(dir, "file-"+strconv.Itoa(i)))
			assert.Nil(t, err)
			assert.False(t, e)
		}
	}
}
