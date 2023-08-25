package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testPathStruct struct {
	path          string
	expected      bool
	expectedError bool
}

func TestProtocol_OS(t *testing.T) {
	name := "/tmp/test"
	tests := []testPathStruct{
		{
			path:     "/tmp/test",
			expected: true,
		},
		{
			path:          "tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "file:///tmp/test",
			expected: false,
		},
		{
			path:     "file:/tmp/test",
			expected: false,
		},
		{
			path:     "err:/tmp/test",
			expected: false,
		},
		{
			path:     "qiniu:/tmp/test",
			expected: false,
		},
		{
			path:     "s3:/tmp/test",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			p, key, err := GetPathProtocol(tt.path)
			msg := fmt.Sprintf("path: %s", tt.path)
			if tt.expectedError {
				assert.NotNil(t, err, msg)
			} else {
				assert.Nil(t, err, msg)
			}
			assert.Equal(t, tt.expected, p == OSProtocol, msg)
			if tt.expected {
				assert.Equal(t, name, key, msg)
			}
		})
	}
}

func TestProtocol_Qiniu(t *testing.T) {
	name := "/tmp/test"
	tests := []testPathStruct{
		{
			path:     "/tmp/test",
			expected: false,
		},
		{
			path:          "tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "file:///tmp/test",
			expected: false,
		},
		{
			path:     "file:/tmp/test",
			expected: false,
		},
		{
			path:     "err:/tmp/test",
			expected: false,
		},
		{
			path:          "qiniu:tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "qiniu:/tmp/test",
			expected: true,
		},
		{
			path:          "qiniu://tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "qiniu:///tmp/test",
			expected: true,
		},
		{
			path:     "s3:/tmp/test",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			p, key, err := GetPathProtocol(tt.path)
			msg := fmt.Sprintf("path: %s", tt.path)
			if tt.expectedError {
				assert.NotNil(t, err, msg)
			} else {
				assert.Nil(t, err, msg)
			}
			assert.Equal(t, tt.expected, p == QiniuProtocol, msg)
			if tt.expected {
				assert.Equal(t, name, key, msg)
			}
		})
	}
}

func TestProtocol_S3(t *testing.T) {
	name := "/tmp/test"
	tests := []testPathStruct{
		{
			path:     "/tmp/test",
			expected: false,
		},
		{
			path:          "tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "file:///tmp/test",
			expected: false,
		},
		{
			path:     "err:/tmp/test",
			expected: false,
		},
		{
			path:          "qiniu:tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "qiniu:/tmp/test",
			expected: false,
		},
		{
			path:     "s3:/tmp/test",
			expected: true,
		},
		{
			path:          "s3:tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:          "s3://tmp/test",
			expected:      false,
			expectedError: true,
		},
		{
			path:     "s3:///tmp/test",
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			p, key, err := GetPathProtocol(tt.path)
			msg := fmt.Sprintf("path: %s", tt.path)
			if tt.expectedError {
				assert.NotNil(t, err, msg)
			} else {
				assert.Nil(t, err, msg)
			}
			assert.Equal(t, tt.expected, p == S3Protocol, msg)
			if tt.expected {
				assert.Equal(t, name, key, msg)
			}
		})
	}
}
