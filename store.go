package store

import (
	"fmt"
	"io"
	"os"

	logger "github.com/ipfs/go-log/v2"
	"github.com/service-sdk/go-sdk-qn/v2/operation"
)

var (
	log              = logger.Logger("store")
	ErrNotConfigured = fmt.Errorf("store is not configured")

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
	if st == nil {
		return nil, ErrNotConfigured
	}
	return st.ListPrefix(p)
}

func (s *Store) UploadData(data []byte, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	if st == nil {
		return ErrNotConfigured
	}
	return st.UploadData(data, p)
}

func (s *Store) Upload(file string, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	if st == nil {
		return ErrNotConfigured
	}
	return st.Upload(file, p)
}

func (s *Store) UploadReader(reader io.Reader, size int64, key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	if st == nil {
		return ErrNotConfigured
	}
	return st.UploadReader(reader, size, p)
}

func (s *Store) DeleteDirectory(dir string) (err error) {
	st, _, p, err := s.getStoreByKey(dir)
	if err != nil {
		return err
	}
	if st == nil {
		return ErrNotConfigured
	}
	return st.DeleteDirectory(p)
}

func (s *Store) Delete(key string) (err error) {
	st, _, p, err := s.getStoreByKey(key)
	if err != nil {
		return err
	}
	if st == nil {
		return ErrNotConfigured
	}
	return st.Delete(p)
}

func (s *Store) Exists(key string) (e bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return false, err
	}
	if st != nil {
		if e1, err := st.Exists(p); err != nil {
			return false, err
		} else if e1 {
			return e1, nil
		}
	}
	if rt != nil {
		return rt.Exists(p)
	}
	return false, nil
}

func (s *Store) Stat(key string) (fs FileStat, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return FileStat{}, err
	}
	if st != nil {
		fs, err = st.Stat(p)
		if err == nil {
			return fs, nil
		}
	}
	if rt != nil {
		fs, err = rt.Stat(p)
		if err == nil {
			return fs, nil
		}
	}
	return
}

func (s *Store) DownloadBytes(key string) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if st != nil {
		data, err = st.DownloadBytes(p)
		if err == nil {
			return data, nil
		}
	}
	if rt != nil {
		data, err = rt.DownloadBytes(p)
		if err == nil {
			return data, nil
		}
	}
	return
}

func (s *Store) DownloadReader(key string) (rc io.ReadCloser, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}

	if st != nil {
		rc, err = st.DownloadReader(p)
		if err == nil {
			return rc, nil
		}
	}
	if rt != nil {
		rc, err = rt.DownloadReader(p)
		if err == nil {
			return rc, nil
		}
	}
	return
}

func (s *Store) DownloadRangeBytes(key string, offset int64, size int64) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if st != nil {
		data, err = st.DownloadRangeBytes(p, offset, size)
		if err == nil {
			return data, nil
		}
	}
	if rt != nil {
		data, err = rt.DownloadRangeBytes(p, offset, size)
		if err == nil {
			return data, nil
		}
	}
	return
}

func (s *Store) DownloadRangeReader(key string, offset int64, size int64) (rc io.ReadCloser, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: %v", r)
		}
	}()
	st, rt, p, err := s.getStoreByKey(key)
	if err != nil {
		return nil, err
	}
	if st != nil {
		rc, err = st.DownloadRangeReader(p, offset, size)
		if err == nil {
			return rc, nil
		}
	}
	if rt != nil {
		rc, err = rt.DownloadRangeReader(p, offset, size)
		if err == nil {
			return rc, nil
		}
	}
	return
}

func (s *Store) getStoreByKey(key string) (Interface, Interface, string, error) {
	pp, p, err := GetPathProtocol(key)
	if err != nil {
		return nil, nil, p, err
	}
	switch pp {
	case QiniuProtocol:
		if s.qiniuStore == nil && s.qiniuReaderStore == nil {
			return nil, nil, p, fmt.Errorf("%s %s", pp, ErrNotConfigured)
		}
		return s.qiniuStore, s.qiniuReaderStore, p, nil
	case S3Protocol:
		if s.s3Store == nil && s.s3ReaderStore == nil {
			return nil, nil, p, fmt.Errorf("%s %s", pp, ErrNotConfigured)
		}
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
		operation.QINIU_MULTI_CLUSTER_ENV,
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
			if env == operation.QINIU_MULTI_CLUSTER_ENV {
				return NewMultiClusterQiniuStore()
			}
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
