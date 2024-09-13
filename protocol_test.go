package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPathProtocol(t *testing.T) {
	tests := []struct {
		input    string
		expected PathProtocol
		path     string
		hasError bool
	}{
		{"qiniu:/file/path", QiniuProtocol, "file/path", false},
		{"s3:/file/path", S3Protocol, "file/path", false},
		{"/file/path", OSProtocol, "/file/path", false},
		{"qiniu://host/file/path", UnknownPathProtocol, "file/path", true},
		{"s3://host/file/path", UnknownPathProtocol, "file/path", true},
		{"unknown://host/path", UnknownPathProtocol, "path", true},
		{"", UnknownPathProtocol, "", true},
	}

	for _, test := range tests {
		protocol, path, err := GetPathProtocol(test.input)
		if test.hasError {
			assert.Error(t, err, "expected error for input: %s", test.input)
		} else {
			assert.NoError(t, err, "unexpected error for input: %s", test.input)
		}
		assert.Equal(t, test.expected, protocol, "unexpected protocol for input: %s", test.input)
		assert.Equal(t, test.path, path, "unexpected path for input: %s", test.input)
	}
}

func TestIsUnionPath(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"qiniu:/file/path", true},
		{"s3:/file/path", true},
		{"/file/path", false},
		{"qiniu://host/file/path", false},
		{"s3://host/file/path", false},
		{"unknown://file/path", false},
	}

	for _, test := range tests {
		result := IsUnionPath(test.input)
		assert.Equal(t, test.expected, result, "unexpected result for input: %s", test.input)
	}
}
