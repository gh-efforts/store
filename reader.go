package store

import (
	"fmt"
	"io"
	"os"

	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	carv2 "github.com/ipld/go-car/v2"
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

type PathReader struct {
	Path      string
	PieceSize abi.UnpaddedPieceSize
	rc        io.ReadCloser
}

func NewPathReader(path string, pieceSize abi.UnpaddedPieceSize) *PathReader {
	return &PathReader{Path: path, PieceSize: pieceSize}
}

func (r *PathReader) Read(p []byte) (n int, err error) {
	if r.rc == nil {
		r.rc, err = openReader(r.Path, r.PieceSize)
		if err != nil {
			return 0, fmt.Errorf("failed to open reader: %w", err)
		}
	}
	return r.rc.Read(p)
}

func (r *PathReader) Close() error {
	if r.rc != nil {
		return r.rc.Close()
	}
	return nil
}

func openReader(filePath string, pieceSize abi.UnpaddedPieceSize) (io.ReadCloser, error) {
	st, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}
	size := uint64(st.Size())

	// Open a reader against the CAR file with the deal data
	v2r, err := carv2.OpenReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CAR reader over %s: %w", filePath, err)
	}
	v2r.Close()

	r, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", filePath, err)
	}

	reader, err := padreader.NewInflator(r, size, pieceSize)
	if err != nil {
		return nil, fmt.Errorf("failed to inflate data: %w", err)
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: reader,
		Closer: r,
	}, nil
}

var _ io.ReadCloser = &PathReader{}
