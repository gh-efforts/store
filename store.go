package store

import (
	"fmt"
	"io"
	"os"

	"github.com/service-sdk/go-sdk-qn/syncdata/operation"

	logger "github.com/ipfs/go-log/v2"
)

var (
	log      = logger.Logger("store")
	s3Env    = "S3_STORE_CONFIG"
	qiniuEnv = "QINIU_STORE_CONFIG"
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

type FileStat struct {
	Size int64
}

func NewStore(qiniuConfigPath, s3ConfigPath string) (Interface, error) {
	store := &Store{
		osStore: NewOSStore(),
	}
	if qiniuConfigPath == "" {
		qiniuConfigPath = os.Getenv(qiniuEnv)
		if qiniuConfigPath == "" {
			qiniuConfigPath = os.Getenv(operation.QINIU_ENV)
		}
	}
	if qiniuConfigPath != "" {
		st, err := NewQiniuStore(qiniuConfigPath)
		if err != nil {
			return nil, fmt.Errorf("new qiniu store error: %v", err)
		}
		store.qiniuStore = st
	}
	if s3ConfigPath == "" {
		s3ConfigPath = os.Getenv(s3Env)
	}
	if s3ConfigPath != "" {
		st, err := NewS3Store(s3ConfigPath)
		if err != nil {
			return nil, fmt.Errorf("new s3 store error: %v", err)
		}
		store.s3Store = st
	}
	return store, nil
}

type Store struct {
	osStore    Interface
	qiniuStore Interface
	s3Store    Interface
}

func (s *Store) ListPrefix(key string) ([]string, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.ListPrefix(p)
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

func (s *Store) Stat(key string) (FileStat, error) {
	st, p, err := s.getStoreByKey(key)
	if err != nil {
		return FileStat{}, err
	}
	return st.Stat(p)
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

func (s *Store) getStoreByKey(key string) (Interface, string, error) {
	pp, p, err := GetPathProtocol(key)
	if err != nil {
		return nil, p, err
	}
	switch pp {
	case QiniuProtocol:
		return s.qiniuStore, p, nil
	case S3Protocol:
		return s.s3Store, p, nil
	case OSProtocol:
		return s.osStore, p, nil
	default:
		return nil, p, fmt.Errorf("unsupported file path protocol: %s, %s", pp, key)
	}
}

var _ Interface = (*Store)(nil)
