package store

import (
	"fmt"
	"net/url"
	"strings"
)

type PathProtocol string

func (p PathProtocol) String() string {
	return string(p)
}

const (
	// QiniuProtocol is the protocol of Qiniu.
	// A file path should start with "qiniu://" or "qiniu:".
	// 1. qiniu://{host}/file/path
	// 2. qiniu:/file/path
	// Check https://en.wikipedia.org/wiki/File_URI_scheme
	QiniuProtocol   PathProtocol = "qiniu"
	S3Protocol      PathProtocol = "s3"
	OSProtocol      PathProtocol = ""
	UnknownProtocol PathProtocol = "Unknown"
)

func GetPathProtocol(p string) (PathProtocol, string, error) {
	u, err := url.Parse(p)
	if err != nil {
		// todo: should panic or handle error?
		return UnknownProtocol, "", err
	}
	if u.Path == "" {
		return UnknownProtocol, "", fmt.Errorf("unsupported path: %s", p)
	}
	if u.Host != "" {
		// maybe it is network path?
		// not supported network protocol for now.
		// todo: support network path
		return UnknownProtocol, "", fmt.Errorf("unsupported network path: %s", p)
	}
	switch u.Scheme {
	case QiniuProtocol.String():
		return QiniuProtocol, u.Path, nil
	case S3Protocol.String():
		return S3Protocol, u.Path, nil
	case OSProtocol.String():
		if strings.HasPrefix(u.Path, "/") {
			return OSProtocol, u.Path, nil
		}
		return UnknownProtocol, u.Path, fmt.Errorf("unsupported path: %s", p)
	default:
		return UnknownProtocol, u.Path, nil
	}
}
