package store

import (
	"fmt"
	"io"

	"github.com/service-sdk/go-sdk-qn/v2/operation"

	logging "github.com/ipfs/go-log/v2"
)

var (
	log              = logging.Logger("store")
	QiNiuEnv         = operation.QINIU_MULTI_CLUSTER_ENV
	S3Env            = "S3_MULTI_CLUSTER_ENV"
	ErrNotConfigured = fmt.Errorf("store is not configured")
)

type Interface interface {
	Stat(key string) (FileStat, error)
	UploadData(data []byte, key string) (err error)
	Upload(file string, key string) (err error)
	UploadReader(reader io.Reader, size int64, key string) (err error)
	DeleteDirectory(dir string) (err error)
	Delete(key string) (err error)
	Exists(key string) (bool, error)
	DownloadBytes(key string) ([]byte, error)
	DownloadReader(key string) (io.ReadCloser, error)
	DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error)
	DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error)
	ListPrefix(key string) ([]string, error)
}

var _ Interface = &Store{}

type FileStat struct {
	Size int64
}

type Store struct {
	osStore    Interface
	qiniuStore Interface
	s3Store    Interface
}

func (s *Store) getStoreByKey(key string) (Interface, string, error) {
	pp, p, err := GetPathProtocol(key)
	if err != nil {
		return nil, p, err
	}
	switch pp {
	case QiniuProtocol:
		if s.qiniuStore == nil {
			return nil, p, fmt.Errorf("%s %s", pp, ErrNotConfigured)
		}
		return s.qiniuStore, p, nil
	case S3Protocol:
		if s.s3Store == nil {
			return nil, p, fmt.Errorf("%s %s", pp, ErrNotConfigured)
		}
		return s.s3Store, p, nil
	case OSProtocol:
		return s.osStore, p, nil
	default:
		return nil, p, fmt.Errorf("unsupported file path protocol: %s, %s", pp, key)
	}
}

func (s *Store) Stat(key string) (FileStat, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return FileStat{}, err
	}
	return st.Stat(p)
}

func (s *Store) UploadData(data []byte, key string) (err error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.UploadData(data, p)
}

func (s *Store) Upload(file string, key string) (err error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.Upload(file, p)
}

func (s *Store) UploadReader(reader io.Reader, size int64, key string) (err error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.UploadReader(reader, size, p)
}

func (s *Store) DeleteDirectory(dir string) (err error) {
	st, p, err := s.getStoreByKey(dir)
	if err != nil {
		return err
	}
	return st.DeleteDirectory(p)
}

func (s *Store) Delete(key string) (err error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.Delete(p)
}

func (s *Store) Exists(key string) (bool, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return false, err
	}
	return st.Exists(p)
}

func (s *Store) DownloadBytes(key string) ([]byte, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadBytes(p)
}

func (s *Store) DownloadReader(key string) (io.ReadCloser, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadReader(p)
}

func (s *Store) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadRangeBytes(p, offset, size)
}

func (s *Store) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.DownloadRangeReader(p, offset, size)
}

func (s *Store) ListPrefix(key string) ([]string, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.ListPrefix(p)
}
