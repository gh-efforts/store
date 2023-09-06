package store

import (
	"fmt"
	"io"
	"os"

	"github.com/service-sdk/go-sdk-qn/syncdata/operation"

	logger "github.com/ipfs/go-log/v2"
)

var (
	log            = logger.Logger("store")
	S3Env          = "S3_STORE_CONFIG"
	S3ReaderEnv    = "S3_READER_CONFIG"
	QiniuEnv       = "QINIU_STORE_CONFIG"
	QiniuReaderEnv = "QINIU_READER_CONFIG"
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
	return newStore(qiniuConfigPath, s3ConfigPath)
}

type Store struct {
	osStore          Interface
	qiniuStore       Interface
	qiniuReaderStore Interface
	s3Store          Interface
	s3ReaderStore    Interface
}

func (s *Store) ListPrefix(key string) ([]string, error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	return st.ListPrefix(p)
}

func (s *Store) UploadData(data []byte, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.UploadData(data, p)
}

func (s *Store) Upload(file string, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.Upload(file, p)
}

func (s *Store) UploadReader(reader io.Reader, size int64, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.UploadReader(reader, size, p)
}

func (s *Store) DeleteDirectory(dir string) (err error) {
	st, _, p, err := s.getStoreByKey(dir)
	if err != nil {
		return err
	}
	return st.DeleteDirectory(p)
}

func (s *Store) Delete(key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	return st.Delete(p)
}

func (s *Store) Exists(key string) (bool, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return false, err
	}
	if e, err := st.Exists(p); err != nil {
		return false, err
	} else if !e && rt != nil {
		return rt.Exists(p)
	} else {
		return e, err
	}
}

func (s *Store) Stat(key string) (FileStat, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return FileStat{}, err
	}
	if fi, err := st.Stat(p); err != nil && rt != nil {
		return rt.Stat(p)
	} else {
		return fi, err
	}
}

func (s *Store) DownloadBytes(key string) ([]byte, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if data, err := st.DownloadBytes(p); err != nil && rt != nil {
		return rt.DownloadBytes(p)
	} else {
		return data, err
	}
}

func (s *Store) DownloadReader(key string) (io.ReadCloser, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if reader, err := st.DownloadReader(p); err != nil && rt != nil {
		return rt.DownloadReader(p)
	} else {
		return reader, err
	}
}

func (s *Store) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if data, err := st.DownloadRangeBytes(p, offset, size); err != nil && rt != nil {
		return rt.DownloadRangeBytes(p, offset, size)
	} else {
		return data, err
	}
}

func (s *Store) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if reader, err := st.DownloadRangeReader(p, offset, size); err != nil && rt != nil {
		return rt.DownloadRangeReader(p, offset, size)
	} else {
		return reader, err
	}
}

func (s *Store) getStoreByKey(key string) (Interface, Interface, string, error) {
	pp, p, err := GetPathProtocol(key)
	if err != nil {
		return nil, nil, p, err
	}
	switch pp {
	case QiniuProtocol:
		return s.qiniuStore, s.qiniuReaderStore, p, nil
	case S3Protocol:
		return s.s3Store, s.s3ReaderStore, p, nil
	case OSProtocol:
		return s.osStore, nil, p, nil
	default:
		return nil, nil, p, fmt.Errorf("unsupported file path protocol: %s, %s", pp, key)
	}
}

func newStore(qiniuConfigPath, s3ConfigPath string) (*Store, error) {

	store := &Store{
		osStore: NewOSStore(),
	}

	// qiniu store
	if qiniuConfigPath == "" {
		qiniuStore, err := newQiniuFromEnv([]string{
			QiniuEnv,
			operation.QINIU_ENV,
		})
		if err != nil {
			return nil, fmt.Errorf("new qiniu store error: %v", err)
		}
		store.qiniuStore = qiniuStore
	} else {
		qiniuStore, err := NewQiniuStore(qiniuConfigPath)
		if err != nil {
			return nil, fmt.Errorf("new qiniu store error: %v", err)
		}
		store.qiniuStore = qiniuStore
	}

	// qiniu reader
	qiniuReader, err := newQiniuFromEnv([]string{
		QiniuReaderEnv,
		"QINIU_READER_CONFIG_PATH",
	})
	if err != nil {
		return nil, fmt.Errorf("new qiniu reader error: %v", err)
	}
	store.qiniuReaderStore = qiniuReader

	// s3 store
	if s3ConfigPath == "" {
		s3Store, err := newS3FromEnv([]string{
			S3Env,
		})
		if err != nil {
			return nil, fmt.Errorf("new s3 store error: %v", err)
		}
		store.s3Store = s3Store
	} else {
		s3Store, err := NewS3Store(s3ConfigPath)
		if err != nil {
			return nil, fmt.Errorf("new s3 store error: %v", err)
		}
		store.s3Store = s3Store
	}

	// s3 reader
	s3Reader, err := newS3FromEnv([]string{
		S3ReaderEnv,
	})
	if err != nil {
		return nil, fmt.Errorf("new s3 reader error: %v", err)
	}
	store.s3ReaderStore = s3Reader

	return store, nil
}

func newQiniuFromEnv(envs []string) (Interface, error) {
	for _, env := range envs {
		val, exists := os.LookupEnv(env)
		if exists {
			return NewQiniuStore(val)
		}
	}
	return nil, nil
}

func newS3FromEnv(envs []string) (Interface, error) {
	for _, env := range envs {
		val, exists := os.LookupEnv(env)
		if exists {
			return NewS3Store(val)
		}
	}
	return nil, nil
}

var _ Interface = (*Store)(nil)
