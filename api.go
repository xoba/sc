package sc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type APICombinator struct {
	e APIEngine
}

type APIEngine interface {
	Get(r Reference) (*http.Response, error)
}

func NewAPICombinator(e APIEngine) (*APICombinator, error) {
	return &APICombinator{e: e}, nil
}

func (a APICombinator) Get(r Reference) (interface{}, error) {
	resp, err := a.e.Get(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		w := new(bytes.Buffer)
		if _, err := io.Copy(w, resp.Body); err != nil {
			return nil, err
		}
		return w.Bytes(), fmt.Errorf("bad status: %q", resp.Status)
	}
	return resp.Body, nil
}

func (a APICombinator) Put(Reference, interface{}) error {
	return unsupported(a, "Put")
}
func (a APICombinator) Delete(Reference) error {
	return unsupported(a, "Delete")
}
func (a APICombinator) Merge(Reference, interface{}) error {
	return unsupported(a, "Merge")
}
