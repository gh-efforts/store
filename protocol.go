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
	// QiniuProtocol represents the Qiniu protocol.
	// A file path should start with "qiniu:" (e.g., qiniu:/file/path).
	// Refer to https://en.wikipedia.org/wiki/File_URI_scheme for more details.
	QiniuProtocol       PathProtocol = "qiniu"
	S3Protocol          PathProtocol = "s3"
	OSProtocol          PathProtocol = ""
	UnknownPathProtocol PathProtocol = "unknown"
)

func GetPathProtocol(p string) (PathProtocol, string, error) {
	u, err := url.Parse(p)
	if err != nil {
		// todo: should panic or handle error?
		return UnknownPathProtocol, "", err
	}
	if u.Path == "" {
		return UnknownPathProtocol, strings.TrimPrefix(u.Path, "/"), fmt.Errorf("unsupported path: %s", p)
	}
	if u.Host != "" {
		// The provided path appears to be a network path.
		// Currently, network protocols are not supported.
		// TODO: Add support for network paths in the future.
		return UnknownPathProtocol, strings.TrimPrefix(u.Path, "/"), fmt.Errorf("unsupported network path: %s", p)
	}
	switch u.Scheme {
	case QiniuProtocol.String():
		return QiniuProtocol, strings.TrimPrefix(u.Path, "/"), nil
	case S3Protocol.String():
		return S3Protocol, strings.TrimPrefix(u.Path, "/"), nil
	case OSProtocol.String():
		if strings.HasPrefix(u.Path, "/") {
			return OSProtocol, u.Path, nil
		}
		return UnknownPathProtocol, u.Path, fmt.Errorf("unsupported path: %s", p)
	default:
		return UnknownPathProtocol, u.Path, nil
	}
}

func IsUnionPath(p string) bool {
	protocol, _, err := GetPathProtocol(p)
	if err != nil {
		return false
	}
	switch protocol {
	case OSProtocol:
		// This implementation is not rigorous. For simplicity, we assume
		// all supported paths are union paths except for OS paths.
		return false
	default:
		return true
	}
}
