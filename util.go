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

// interprets and shows something we get from a storage combinator
func Show(i interface{}) (string, error) {
	reader := func(r io.Reader) (string, error) {
		w := new(bytes.Buffer)
		if _, err := io.Copy(w, r); err != nil {
			return "", err
		}
		return w.String(), nil
	}
	encode := func(i interface{}) (string, error) {
		t := reflect.TypeOf(i)
		v := reflect.ValueOf(i)
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		switch t.Kind() {
		case reflect.Slice, reflect.Array:
			for j := 0; j < v.Len(); j++ {
				if err := e.Encode(v.Index(j).Interface()); err != nil {
					return "", err
				}
			}
		default:
			if err := e.Encode(i); err != nil {
				return "", err
			}
		}
		return w.String(), nil
	}
	switch t := i.(type) {
	case []byte:
		return string(t), nil
	case string:
		return t, nil
	case io.ReadCloser:
		defer t.Close()
		return reader(t)
	case io.Reader:
		return reader(t)
	case []interface{}, []FileReference, Versions:
		return encode(t)
	default:
		return fmt.Sprint(t), nil
	}
}
