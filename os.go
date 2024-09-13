package store

import (
	"fmt"
	"io"
	"os"
	"path"
)

func NewOSStore() Interface {
	return &OSStore{}
}

type OSStore struct {
}

func (s *OSStore) ListPrefix(key string) (keys []string, err error) {
	fi, err := os.Stat(key)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		keys = append(keys, path.Join(key, fi.Name()))
		return
	}
	files, err := os.ReadDir(key)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		keys = append(keys, path.Join(key, file.Name()))
	}
	return
}

// Stat returns a FileStat for the given key.
func (s *OSStore) Stat(key string) (FileStat, error) {
	fileInfo, err := os.Stat(key)
	if err != nil {
		return FileStat{}, err
	}
	return FileStat{
		Size: fileInfo.Size(),
	}, nil
}

// UploadData writes data to the given file.
// If the file already exists, it will return an error.
func (s *OSStore) UploadData(data []byte, key string) (err error) {
	dir := path.Dir(key)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	_, err = os.Stat(key)
	if !os.IsNotExist(err) {
		if err == nil {
			// file exists
			return fmt.Errorf("file %s already exists", key)
		}
		return err
	}
	if err := os.WriteFile(key, data, 0644); err != nil {
		return fmt.Errorf("write file %s error: %s", key, err.Error())
	}
	return nil
}

// Upload "upload local file to local", it means just copy the file.
func (s *OSStore) Upload(file string, key string) (err error) {
	e, err := s.Exists(key)
	if err != nil {
		return fmt.Errorf("check exists: %s", err)
	}
	if e {
		return fmt.Errorf("file %s already exists", key)
	}
	src, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("open file %s error: %s", file, err)
	}
	defer src.Close() // nolint: errcheck
	dest, err := os.Create(key)
	if err != nil {
		return fmt.Errorf("create file %s error: %s", key, err)
	}
	defer dest.Close() // nolint: errcheck
	_, err = io.Copy(dest, src)
	if err != nil {
		return fmt.Errorf("copy file %s error: %s", key, err)
	}
	return nil
}

// UploadReader writes the reader to a file.
func (s *OSStore) UploadReader(reader io.Reader, _ int64, key string) (err error) {
	dir := path.Dir(key)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	_, err = os.Stat(key)
	if !os.IsNotExist(err) {
		if err == nil {
			// file exists
			return fmt.Errorf("file %s already exists", key)
		}
		return err
	}
	file, err := os.Create(key)
	if err != nil {
		return fmt.Errorf("create file %s error: %s", key, err)
	}
	defer file.Close() // nolint: errcheck
	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("write file %s error: %s", key, err)
	}
	return nil
}

// DeleteDirectory deletes a directory and all of its contents.
// If the directory is empty, return nil.
func (s *OSStore) DeleteDirectory(dir string) (err error) {
	st, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return os.RemoveAll(dir)
}

// Delete removes a file.
// with same behavior as os.Remove.
func (s *OSStore) Delete(key string) (err error) {
	return os.Remove(key)
}

// Exists checks if a file exists.
func (s *OSStore) Exists(key string) (bool, error) {
	_, err := os.Stat(key)
	if !os.IsNotExist(err) {
		if err == nil {
			// file exists
			return true, nil
		}
		// other error
		return false, err
	}
	return false, nil
}

func (s *OSStore) DownloadBytes(key string) ([]byte, error) {
	f, err := os.Open(key)
	if err != nil {
		return nil, err
	}
	defer f.Close() // nolint: errcheck
	return io.ReadAll(f)
}

func (s *OSStore) DownloadReader(key string) (io.ReadCloser, error) {
	f, err := os.Open(key)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *OSStore) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	f, err := os.Open(key)
	if err != nil {
		return nil, err
	}
	defer f.Close() // nolint: errcheck
	no, err := f.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	if offset != no {
		return nil, fmt.Errorf("seek offset not matched, expected %d, got %d", offset, no)
	}
	buf := make([]byte, size)
	n, err := f.Read(buf)
	if err != nil {
		return nil, err
	}
	if int64(n) != size {
		return nil, fmt.Errorf("read size not matched, expected %d, got %d", size, n)
	}
	return buf, nil
}

type rangeReaderCloser struct {
	io.Reader
	closer func() error
}

func (r *rangeReaderCloser) Close() error {
	return r.closer()
}

func (s *OSStore) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	f, err := os.Open(key)
	if err != nil {
		return nil, err
	}
	no, err := f.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	if offset != no {
		return nil, fmt.Errorf("seek offset not matched, expected %d, got %d", offset, no)
	}

	return &rangeReaderCloser{io.LimitReader(f, size), f.Close}, nil
}

var _ Interface = &OSStore{}
