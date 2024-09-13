package store

import (
	"io"
	"os"
)

type S3MultiStore struct {
	cfg *S3MultiStoreConfig
}

func NewS3MultiStore(cfgPath string) (Interface, error) {
	cfg, err := LoadS3MultiStoreConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	return &S3MultiStore{cfg: cfg}, nil
}

// NewS3MultiStoreWithEnv creates a new S3MultiStore with the given environment variable name.
func NewS3MultiStoreWithEnv() (Interface, error) {
	cfgPath, ok := os.LookupEnv(S3Env)
	if !ok {
		return nil, S3NotConfigError
	}
	return NewS3MultiStore(cfgPath)
}

func (s *S3MultiStore) Stat(key string) (FileStat, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return FileStat{}, err
	}
	return st.Stat(key)
}

func (s *S3MultiStore) UploadData(data []byte, key string) (err error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return err
	}
	return st.UploadData(data, key)
}

func (s *S3MultiStore) Upload(file string, key string) (err error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return err
	}
	return st.Upload(file, key)
}

func (s *S3MultiStore) UploadReader(reader io.Reader, size int64, key string) (err error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return err
	}
	return st.UploadReader(reader, size, key)
}

func (s *S3MultiStore) DeleteDirectory(dir string) (err error) {
	st, err := s.cfg.getStore(dir)
	if err != nil {
		return err
	}
	return st.DeleteDirectory(dir)
}

func (s *S3MultiStore) Delete(key string) (err error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return err
	}
	return st.Delete(key)
}

func (s *S3MultiStore) Exists(key string) (bool, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return false, err
	}
	return st.Exists(key)
}

func (s *S3MultiStore) DownloadBytes(key string) ([]byte, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadBytes(key)
}

func (s *S3MultiStore) DownloadReader(key string) (io.ReadCloser, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadReader(key)
}

func (s *S3MultiStore) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadRangeBytes(key, offset, size)
}

func (s *S3MultiStore) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadRangeReader(key, offset, size)
}

func (s *S3MultiStore) ListPrefix(key string) ([]string, error) {
	st, err := s.cfg.getStore(key)
	if err != nil {
		return nil, err
	}
	return st.ListPrefix(key)
}
