package sc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// a simple reference type
type Ref struct {
	uri *url.URL
}

func (r Ref) String() string {
	return r.uri.String()
}

func NewRef(p string) Ref {
	var r Ref
	r.uri = &url.URL{}
	r.uri.Path = p
	return r
}

func NewURI(u *url.URL) Ref {
	return Ref{
		uri: u,
	}
}

func ParseRef(p string) (*Ref, error) {
	if strings.TrimSpace(p) == "" {
		return nil, errors.New("empty uri")
	}
	u, err := url.Parse(p)
	if err != nil {
		return nil, err
	}
	return &Ref{
		uri: u,
	}, nil
}

func RemoveFragment(r Reference) (Reference, error) {
	return Remove(r, func(u *url.URL) error {
		u.Fragment = ""
		return nil
	})
}

func RemoveQueryParameter(r Reference, key string) (Reference, error) {
	return Remove(r, func(u *url.URL) error {
		q := u.Query()
		q.Set(key, "")
		u.RawQuery = q.Encode()
		return nil
	})
}

func Remove(r Reference, mutator func(*url.URL) error) (Reference, error) {
	r2, err := ParseRef(r.URI().String())
	if err != nil {
		return nil, err
	}
	u := r2.URI()
	if err := mutator(u); err != nil {
		return nil, err
	}
	return NewURI(u), nil
}

func RemoveQuery(r Reference) (Reference, error) {
	return Remove(r, func(u *url.URL) error {
		u.RawQuery = ""
		return nil
	})

}

func (r Ref) URI() *url.URL {
	return r.uri
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

// interprets as bytes something we get from a storage combinator
func Blob(i interface{}) ([]byte, error) {
	cp := func(r io.Reader) ([]byte, error) {
		w := new(bytes.Buffer)
		if _, err := io.Copy(w, r); err != nil {
			return nil, err
		}
		return w.Bytes(), nil
	}
	encode := func(i interface{}) ([]byte, error) {
		t := reflect.TypeOf(i)
		v := reflect.ValueOf(i)
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		e.SetEscapeHTML(false)
		switch t.Kind() {
		case reflect.Slice, reflect.Array:
			for j := 0; j < v.Len(); j++ {
				if err := e.Encode(v.Index(j).Interface()); err != nil {
					return nil, err
				}
			}
		default:
			if err := e.Encode(i); err != nil {
				return nil, err
			}
		}
		return w.Bytes(), nil
	}
	switch t := i.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case io.Reader:
		return cp(t)
	case io.ReadCloser:
		x, err := cp(t)
		if err != nil {
			return nil, err
		}
		if err := t.Close(); err != nil {
			return nil, err
		}
		return x, nil
	case Observations, []interface{}, []FileReference, Versions, []S3Record:
		return encode(t)
	default:
		return nil, fmt.Errorf("can't handle type %T", t)
	}
}

const HashPrefix = `EAE18B82-F047-4913-BFE7-CF5B9E3B35AB`
