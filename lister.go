package sc

import (
	"bytes"
	"encoding/json"
	"net/url"
	"time"
)

type ListingCombinator struct {
	raw, list     StorageCombinator
	listReference Reference
	listPath      string
}

func NewListingCombinator(raw, list StorageCombinator, listPath string) (*ListingCombinator, error) {
	r, err := list.Find(listPath)
	if err != nil {
		return nil, err
	}
	return &ListingCombinator{
		raw:           raw,
		list:          list,
		listReference: r,
		listPath:      listPath,
	}, nil
}

func (lc ListingCombinator) Find(p string) (Reference, error) {
	if p == lc.listPath {
		return lc.listReference, nil
	}
	return lc.raw.Find(p)
}

func (lc ListingCombinator) Get(r Reference) (interface{}, error) {
	if r.URI() == lc.listReference.URI() {
		return lc.list.Get(r)
	}
	return lc.raw.Get(r)
}

type ListRecord struct {
	Time   time.Time
	URI    *url.URL
	Delete bool `json:",omitempty"`
}

func (lc ListingCombinator) update(r Reference, delete bool) error {
	lr := ListRecord{
		Time:   time.Now(),
		URI:    r.URI(),
		Delete: delete,
	}
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)
	e.SetEscapeHTML(false)
	e.SetIndent("", "  ")
	if err := e.Encode(lr); err != nil {
		return err
	}
	return lc.list.Merge(lc.listReference, w.Bytes())
}

func (lc ListingCombinator) Put(r Reference, i interface{}) error {
	if err := lc.update(r, false); err != nil {
		return err
	}
	return lc.raw.Put(r, i)
}

func (lc ListingCombinator) Delete(r Reference) error {
	if err := lc.update(r, true); err != nil {
		return err
	}
	return lc.raw.Delete(r)
}

func (lc ListingCombinator) Merge(r Reference, i interface{}) error {
	if err := lc.update(r, false); err != nil {
		return err
	}
	return lc.raw.Merge(r, i)
}
