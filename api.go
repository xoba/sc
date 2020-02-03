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
	Get(Reference) (*http.Response, error)
	Process(io.ReadCloser) (interface{}, error)
}

func NewAPICombinator(e APIEngine) *APICombinator {
	return &APICombinator{e: e}
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
	return a.e.Process(resp.Body)
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
