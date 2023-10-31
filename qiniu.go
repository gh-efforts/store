package store

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/service-sdk/go-sdk-qn/v2/operation"
)

var (
	QiniuNotConfigError = fmt.Errorf("qiniu is not configured")
)

func NewQiniuStore(cfgPath string) (Interface, error) {
	cfg, err := operation.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	return &QiniuStore{
		downloader: operation.NewDownloader(cfg),
		uploader:   operation.NewUploader(cfg),
		lister:     operation.NewLister(cfg),
	}, nil
}

// NewMultiClusterQiniuStore initializes a multi-cluster qiniu store,
// only support QINIU_MULTI_CLUSTER environment variable
func NewMultiClusterQiniuStore() (Interface, error) {
	if _, e := os.LookupEnv(operation.QINIU_MULTI_CLUSTER_ENV); !e {
		return nil, QiniuNotConfigError
	}
	return &QiniuStore{
		downloader: operation.NewDownloaderV2(),
		uploader:   operation.NewUploaderV2(),
		lister:     operation.NewListerV2(),
	}, nil
}

type QiniuStore struct {
	downloader *operation.Downloader
	uploader   *operation.Uploader
	lister     *operation.Lister
}

func (s *QiniuStore) ListPrefix(key string) ([]string, error) {
	if s == nil {
		return nil, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("list prefix", "key", key, "took", time.Since(start))
	}()
	return s.lister.ListPrefix(key), nil
}

// UploadData upload memory data to Qiniu store.
func (s *QiniuStore) UploadData(data []byte, key string) (err error) {
	if s == nil {
		return QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("upload data", "key", key, "took", time.Since(start))
	}()
	return s.uploader.UploadData(data, key)
}

// Upload uploads a file to qiniu.
func (s *QiniuStore) Upload(file string, key string) (err error) {
	if s == nil {
		return QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("upload", "file", file, "key", key, "took", time.Since(start))
	}()
	return s.uploader.Upload(file, key)
}

// UploadReader upload reader to the Qiniu store.
func (s *QiniuStore) UploadReader(reader io.Reader, _ int64, key string) (err error) {
	if s == nil {
		return QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("upload reader", "key", key, "took", time.Since(start))
	}()
	return s.uploader.UploadReader(reader, key)
}

// DeleteDirectory deletes a directory from the Qiniu store.
func (s *QiniuStore) DeleteDirectory(dir string) (err error) {
	if s == nil {
		return QiniuNotConfigError
	}
	dir = strings.TrimPrefix(dir, "/")
	start := time.Now()
	defer func() {
		log.Debugw("delete directory", "dir", dir, "took", time.Since(start))
	}()
	_, err = s.lister.DeleteDirectory(dir)
	return
}

// Delete deletes a file from the Qiniu store.
func (s *QiniuStore) Delete(key string) (err error) {
	if s == nil {
		return QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("delete", "key", key, "took", time.Since(start))
	}()
	return s.lister.Delete(key)
}

// Exists returns true if the key exists in the Qiniu store.
func (s *QiniuStore) Exists(key string) (bool, error) {
	if s == nil {
		return false, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("check exists", "key", key, "took", time.Since(start))
	}()
	_, err := s.downloader.DownloadCheck(key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// Stat returns the FileInfo for the named file.
func (s *QiniuStore) Stat(key string) (FileStat, error) {
	if s == nil {
		return FileStat{}, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("get stat", "key", key, "took", time.Since(start))
	}()
	n, err := s.downloader.DownloadCheck(key)
	if err != nil {
		return FileStat{}, err
	}
	return FileStat{
		Size: n,
	}, nil
}

func (s *QiniuStore) DownloadBytes(key string) ([]byte, error) {
	if s == nil {
		return nil, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("download bytes", "key", key, "took", time.Since(start))
	}()
	return s.downloader.DownloadBytes(key)
}

func (s *QiniuStore) DownloadReader(key string) (io.ReadCloser, error) {
	if s == nil {
		return nil, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("download reader", "key", key, "took", time.Since(start))
	}()
	resp, err := s.downloader.DownloadRaw(key, http.Header{})
	if err != nil || resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, err)
	}
	return resp.Body, nil
}

func (s *QiniuStore) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	if s == nil {
		return nil, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("download range bytes", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()
	_, data, err := s.downloader.DownloadRangeBytes(key, offset, size)
	return data, err
}

func (s *QiniuStore) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	if s == nil {
		return nil, QiniuNotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("download range reader", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()
	_, reader, err := s.downloader.DownloadRangeReader(key, offset, size)
	return reader, err
}

var _ Interface = &QiniuStore{}
