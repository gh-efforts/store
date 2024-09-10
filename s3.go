package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pelletier/go-toml"
)

const (
	recyclePath = "_recycle/"
)

var (
	S3NotConfigError = fmt.Errorf("s3 store is not configured")
)

type S3Config struct {
	Endpoint  string `json:"endpoint" yaml:"endpoint" toml:"endpoint"`
	Region    string `json:"region" yaml:"region" toml:"region"`
	Bucket    string `json:"bucket" yaml:"bucket" toml:"bucket"`
	AccessKey string `json:"access_key" yaml:"access_key" toml:"access_key"`
	SecretKey string `json:"secret_key" yaml:"secret_key" toml:"secret_key"`
	Token     string `json:"token" yaml:"token" toml:"token"`
	UseSSL    bool   `json:"use_ssl" yaml:"use_ssl" toml:"use_ssl"`
}

func LoadS3Config(cfgPath string) (*S3Config, error) {
	var cfg S3Config
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(path.Ext(cfgPath))
	if ext == ".json" {
		err = json.Unmarshal(raw, &cfg)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &cfg)
	} else {
		return nil, fmt.Errorf("invalid s3 configuration format")
	}
	return &cfg, err
}

type S3Store struct {
	cfg    *S3Config
	client *minio.Client
}

func NewS3Store(cfgPath string) (Interface, error) {
	cfg, err := LoadS3Config(cfgPath)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.Token),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize s3 client: %v", err)
	}
	return &S3Store{
		cfg:    cfg,
		client: client,
	}, nil
}

func (s *S3Store) UploadData(data []byte, key string) (err error) {
	if s == nil {
		return S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")
	opts := minio.PutObjectOptions{}

	info, err := s.client.PutObject(context.TODO(), s.cfg.Bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return fmt.Errorf("upload data: %v", err)
	}
	log.Debugw("uploaded data", "key", key, "size", info.Size, "took", time.Since(start))
	return nil
}

func (s *S3Store) Upload(file string, key string) (err error) {
	if s == nil {
		return S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")
	opts := minio.PutObjectOptions{}

	info, err := s.client.FPutObject(context.TODO(), s.cfg.Bucket, key, file, opts)
	if err != nil {
		return fmt.Errorf("upload file: %v", err)
	}
	log.Debugw("uploaded file", "key", key, "file", file, "size", info.Size, "took", time.Since(start))
	return nil
}

func (s *S3Store) UploadReader(reader io.Reader, size int64, key string) (err error) {
	if s == nil {
		return S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")
	opts := minio.PutObjectOptions{}

	info, err := s.client.PutObject(context.TODO(), s.cfg.Bucket, key, reader, size, opts)
	if err != nil {
		return fmt.Errorf("upload reader: %v", err)
	}
	log.Debugw("uploaded reader", "key", key, "size", info.Size, "took", time.Since(start))
	return nil
}

// DeleteDirectory removes the directory from the s3 store.
// This is a soft-delete operation, all files will be renamed to .
func (s *S3Store) DeleteDirectory(dir string) (err error) {
	if s == nil {
		return S3NotConfigError
	}
	start := time.Now()
	dir = makeSureKeyAsDir(strings.TrimPrefix(dir, "/"))
	opts := minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    dir,
	}
	log.Debugw("delete directory", "dir", dir)
	objectsCh := s.client.ListObjects(context.TODO(), s.cfg.Bucket, opts)
	for obj := range objectsCh {
		log.Debugw("delete object", "key", obj.Key, "size", obj.Size)
		objStart := time.Now()
		dest := minio.CopyDestOptions{
			Bucket: s.cfg.Bucket,
			Object: path.Join(recyclePath, obj.Key),
		}
		src := minio.CopySrcOptions{
			Bucket: s.cfg.Bucket,
			Object: obj.Key,
		}

		info, copyErr := s.client.CopyObject(context.TODO(), dest, src)
		if copyErr != nil {
			err = copyErr
			break
		}
		removeErr := s.client.RemoveObject(context.TODO(), src.Bucket, src.Object, minio.RemoveObjectOptions{})
		if removeErr != nil {
			err = removeErr
			break
		}
		log.Debugw("deleted object", "key", obj.Key, "size", info.Size, "took", time.Since(objStart))
	}
	if err != nil {
		log.Errorf("delete object failed: %v", err)
		// consume the rest
		for range objectsCh {
		}
	}
	log.Debugw("deleted directory", "key", dir, "took", time.Since(start))
	return err
}

// Delete deletes the object.
// This is soft-delete operation, file will be renamed to recyclePath.
func (s *S3Store) Delete(key string) (err error) {
	if s == nil {
		return S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")

	dest := minio.CopyDestOptions{
		Bucket: s.cfg.Bucket,
		Object: path.Join(recyclePath, key),
	}
	src := minio.CopySrcOptions{
		Bucket: s.cfg.Bucket,
		Object: key,
	}
	info, err := s.client.CopyObject(context.TODO(), dest, src)
	if err != nil {
		return fmt.Errorf("copy object: %v", err)
	}
	err = s.client.RemoveObject(context.TODO(), src.Bucket, src.Object, minio.RemoveObjectOptions{})
	log.Debugw("deleted object", "key", key, "size", info.Size, "took", time.Since(start))
	return err
}

// Exists checks if the object exists.
func (s *S3Store) Exists(key string) (bool, error) {
	if s == nil {
		return false, S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")
	_, err := s.client.StatObject(context.TODO(), s.cfg.Bucket, key, minio.StatObjectOptions{})
	if err == nil {
		log.Debugw("object exists", "key", key, "took", time.Since(start))
		return true, nil
	}
	log.Debugw("object not exists", "key", key, "took", time.Since(start))
	return false, nil
}

func (s *S3Store) Stat(key string) (FileStat, error) {
	if s == nil {
		return FileStat{}, S3NotConfigError
	}
	start := time.Now()
	key = strings.TrimPrefix(key, "/")

	info, err := s.client.StatObject(context.TODO(), s.cfg.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return FileStat{}, fmt.Errorf("stat object: %v", err)
	}
	log.Debugw("stat object", "key", key, "size", info.Size, "took", time.Since(start))
	return FileStat{
		Size: info.Size,
	}, nil
}

func (s *S3Store) DownloadRangeBytes(key string, offset int64, size int64) ([]byte, error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	start := time.Now()
	obj, err := s.getObject(key, &offset, &size)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := obj.Close(); err != nil {
			log.Errorf("close object failed: %v", err)
		}
		log.Debugw("downloaded object range", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()

	return io.ReadAll(obj)
}

func (s *S3Store) DownloadBytes(key string) ([]byte, error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	start := time.Now()
	obj, err := s.getObject(key, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := obj.Close(); err != nil {
			log.Errorf("close object failed: %v", err)
		}
		log.Debugw("downloaded object", "key", key, "took", time.Since(start))
	}()

	return io.ReadAll(obj)
}

func (s *S3Store) DownloadReader(key string) (io.ReadCloser, error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	start := time.Now()
	defer func() {
		log.Debugw("downloaded reader", "key", key, "took", time.Since(start))
	}()
	return s.getObject(key, nil, nil)
}

func (s *S3Store) DownloadRangeReader(key string, offset int64, size int64) (io.ReadCloser, error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	start := time.Now()
	defer func() {
		log.Debugw("downloaded range reader", "key", key, "offset", offset, "size", size, "took", time.Since(start))
	}()
	return s.getObject(key, &offset, &size)
}

func (s *S3Store) ListPrefix(key string) (keys []string, err error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	start := time.Now()
	defer func() {
		log.Debugw("listed prefix", "key", key, "took", time.Since(start))
	}()
	key = strings.TrimPrefix(key, "/")
	opts := minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: true,
	}
	for obj := range s.client.ListObjects(context.TODO(), s.cfg.Bucket, opts) {
		keys = append(keys, obj.Key)
	}
	return
}

func (s *S3Store) getObject(key string, offset *int64, size *int64) (*minio.Object, error) {
	if s == nil {
		return nil, S3NotConfigError
	}
	key = strings.TrimPrefix(key, "/")
	opts := minio.GetObjectOptions{}
	if offset != nil || size != nil {
		var start, end int64
		if offset != nil {
			start = *offset
		}
		if size != nil {
			end = start + *size - 1
		}
		if err := opts.SetRange(start, end); err != nil {
			return nil, fmt.Errorf("set range: %v", err)
		}
	}
	return s.client.GetObject(context.TODO(), s.cfg.Bucket, key, opts)
}

var _ Interface = &S3Store{}

func makeSureKeyAsDir(key string) string {
	if strings.HasSuffix(key, "/") {
		return key
	}
	return key + "/"
}
