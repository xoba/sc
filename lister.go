package sc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type ListingCombinator struct {
	raw           StorageCombinator // underlying combinator
	appender      StorageCombinator // managing the listing
	listPath      string            // path the list can be found under
	listReference Reference         // reference to listPath
}

// appender's merge method just appends to growing file
func NewListingCombinator(raw, appender StorageCombinator, listPath string) (*ListingCombinator, error) {
	r, err := appender.Find(listPath)
	if err != nil {
		return nil, err
	}
	return &ListingCombinator{
		raw:           raw,
		appender:      appender,
		listPath:      listPath,
		listReference: r,
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
		return lc.appender.Get(r)
	} else {
		fmt.Printf("ListingCombinator: %q vs %q\n", r.URI(), lc.listReference)
	}
	return lc.raw.Get(r)
}

type ListRecord struct {
	Time time.Time
	URI  string
	Mode string
}

func (lc ListingCombinator) update(r Reference, mode string) error {
	lr := ListRecord{
		Time: time.Now(),
		URI:  r.URI().String(),
		Mode: mode,
	}
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)
	e.SetEscapeHTML(false)
	if err := e.Encode(lr); err != nil {
		return err
	}
	return lc.appender.Merge(lc.listReference, w.Bytes())
}

func (lc ListingCombinator) Put(r Reference, i interface{}) error {
	if list := lc.listReference.URI().String(); r.URI().String() == list {
		return fmt.Errorf("path conflict with listing: %s", list)
	}
	if err := lc.update(r, "put"); err != nil {
		return err
	}
	return lc.raw.Put(r, i)
}

func (lc ListingCombinator) Delete(r Reference) error {
	if err := lc.update(r, "delete"); err != nil {
		return err
	}
	return lc.raw.Delete(r)
}

func (lc ListingCombinator) Merge(r Reference, i interface{}) error {
	if err := lc.update(r, "merge"); err != nil {
		return err
	}
	return lc.raw.Merge(r, i)
}
