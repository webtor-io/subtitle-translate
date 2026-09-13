package services

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

// TestIsNotFound covers the three shapes a missing object comes back as:
// the GET code, the HEAD code, and a bare 404 from an implementation that
// sends neither.
func TestIsNotFound(t *testing.T) {
	notFound := []error{
		awserr.New(s3.ErrCodeNoSuchKey, "The specified key does not exist.", nil),
		awserr.New("NotFound", "Not Found", nil),
		awserr.NewRequestFailure(awserr.New("SomethingElse", "nope", nil), http.StatusNotFound, "req-1"),
		// Wrapping must not hide it.
		errors.Wrap(awserr.New(s3.ErrCodeNoSuchKey, "gone", nil), "s3 get"),
	}
	for i, err := range notFound {
		if !isNotFound(err) {
			t.Errorf("case %d: %v must read as absent", i, err)
		}
	}
	present := []error{
		nil,
		errors.New("connection reset"),
		awserr.New(s3.ErrCodeNoSuchBucket, "no bucket", nil),
		awserr.NewRequestFailure(awserr.New("InternalError", "boom", nil), http.StatusInternalServerError, "req-2"),
	}
	for i, err := range present {
		if isNotFound(err) {
			t.Errorf("case %d: %v must not read as absent", i, err)
		}
	}
}
