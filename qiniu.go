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

type QiniuStore struct {
	downloader *operation.Downloader
	uploader   *operation.Uploader
	lister     *operation.Lister
}

func NewQiniuStore() (Interface, error) {
	if _, e := os.LookupEnv(QiNiuEnv); !e {
		return nil, QiniuNotConfigError
	}
	return &QiniuStore{
		downloader: operation.NewDownloaderV2(),
		uploader:   operation.NewUploaderV2(),
		lister:     operation.NewListerV2(),
	}, nil
}

func (s *QiniuStore) UploadData(data []byte, key string) (err error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("UploadData", "key", key, "took", time.Since(start))
	}()
	return s.uploader.UploadData(data, key)
}

func (s *QiniuStore) Upload(file string, key string) (err error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("Upload", "file", file, "key", key, "took", time.Since(start))
	}()
	return s.uploader.Upload(file, key)
}

func (s *QiniuStore) UploadReader(reader io.Reader, _ int64, key string) (err error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("UploadReader", "key", key, "took", time.Since(start))
	}()
	return s.uploader.UploadReader(reader, key)
}

func (s *QiniuStore) DeleteDirectory(dir string) (err error) {
	dir = strings.TrimPrefix(dir, "/")
	start := time.Now()
	defer func() {
		log.Debugw("DeleteDirectory", "dir", dir, "took", time.Since(start))
	}()
	_, err = s.lister.DeleteDirectory(dir)
	return
}

func (s *QiniuStore) Delete(key string) (err error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("Delete", "key", key, "took", time.Since(start))
	}()
	return s.lister.Delete(key)
}

func (s *QiniuStore) Exists(key string) (bool, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("Exists", "key", key, "took", time.Since(start))
	}()
	_, err := s.downloader.DownloadCheck(key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (s *QiniuStore) DownloadBytes(key string) ([]byte, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("DownloadBytes", "key", key, "took", time.Since(start))
	}()
	return s.downloader.DownloadBytes(key)
}

func (s *QiniuStore) DownloadReader(key string) (io.ReadCloser, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("DownloadReader", "key", key, "took", time.Since(start))
	}()
	resp, err := s.downloader.DownloadRaw(key, http.Header{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, err)
	}
	return resp.Body, nil
}

func (s *QiniuStore) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("DownloadRangeBytes", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()
	_, data, err := s.downloader.DownloadRangeBytes(key, offset, size)
	return data, err
}

func (s *QiniuStore) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("DownloadRangeReader", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()
	_, reader, err := s.downloader.DownloadRangeReader(key, offset, size)
	return reader, err
}

func (s *QiniuStore) ListPrefix(key string) ([]string, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("ListPrefix", "key", key, "took", time.Since(start))
	}()
	return s.lister.ListPrefix(key), nil
}

func (s *QiniuStore) Stat(key string) (FileStat, error) {
	key = strings.TrimPrefix(key, "/")
	start := time.Now()
	defer func() {
		log.Debugw("Stat", "key", key, "took", time.Since(start))
	}()
	n, err := s.downloader.DownloadCheck(key)
	if err != nil {
		return FileStat{}, err
	}
	return FileStat{
		Size: n,
	}, nil
}

var _ Interface = &QiniuStore{}
