package sc

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// a simple reference type
type Ref struct {
	u *url.URL
}

func (r Ref) String() string {
	return r.u.String()
}

func NewRef(p string) Ref {
	var r Ref
	r.u = &url.URL{}
	r.u.Path = p
	return r
}

func NewURI(u *url.URL) Ref {
	return Ref{
		u: u,
	}
}

func ParseRef(p string) (*Ref, error) {
	u, err := url.Parse(p)
	if err != nil {
		return nil, err
	}
	return &Ref{
		u: u,
	}, nil
}

func (r Ref) URI() *url.URL {
	return r.u
}

func unimplemented(i interface{}, method string) error {
	return fmt.Errorf("%T.%s unimplemented; %w", i, method, NotSupported)
}

func unsupported(i interface{}, method string) error {
	return fmt.Errorf("%T.%s unsupported; %w", i, method, NotSupported)
}

// wraps not found error from various sources
func wrapNotFound(r Reference, err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		err = fmt.Errorf("%w (%v)", NotFound, r)
	}
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchBucket:
			err = fmt.Errorf("%w (no such bucket; %v)", NotFound, r)
		case s3.ErrCodeNoSuchKey:
			err = fmt.Errorf("%w (no such key; %v)", NotFound, r)
		}
	}
	return err
}
