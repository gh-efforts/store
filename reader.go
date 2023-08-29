package store

import (
	"fmt"
	"io"
	"os"
)

type Reader struct {
	store  Interface
	Key    string
	Offset *int64
	Size   *int64
	closed bool
	body   io.ReadCloser
}

func NewReader(st Interface, key string, offset *int64, size *int64) *Reader {
	return &Reader{
		store:  st,
		Key:    key,
		Offset: offset,
		Size:   size,
	}
}

func (r *Reader) SeekStart() error {
	return nil
}

func (r *Reader) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (r *Reader) Close() error {
	if !r.closed {
		r.closed = true

		if r.body != nil {
			return r.body.Close()
		}
	}
	return nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, fmt.Errorf("file reader closed")
	}
	if r.body == nil {
		if r.store == nil {
			qiniuCfg, qok := os.LookupEnv(QiniuReaderEnv)
			s3Config, sok := os.LookupEnv(S3ReaderEnv)
			if qok || sok {
				st, err := NewStore(qiniuCfg, s3Config)
				if err != nil {
					return 0, err
				}
				r.store = st
			} else {
				st, err := NewStore("", "")
				if err != nil {
					return 0, err
				}
				r.store = st
			}
		}
		if r.Offset == nil {
			rc, err := r.store.DownloadReader(r.Key)
			if err != nil {
				return 0, err
			}
			r.body = rc
		} else {
			rc, err := r.store.DownloadRangeReader(r.Key, *r.Offset, *r.Size)
			if err != nil {
				return 0, err
			}
			r.body = rc
		}
	}

	return r.body.Read(p)
}
