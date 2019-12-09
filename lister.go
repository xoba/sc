package sc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type ListingCombinator struct {
	raw           StorageCombinator // underlying combinator
	listReference Reference         // where the list can be found
}

// embedded combinator's merge method has to simply append
func NewListingCombinator(raw StorageCombinator, listReference Reference) (*ListingCombinator, error) {
	return &ListingCombinator{
		raw:           raw,
		listReference: listReference,
	}, nil
}

func (lc ListingCombinator) Get(r Reference) (interface{}, error) {
	if r.URI().String() == lc.listReference.URI().String() {
		return lc.raw.Get(r)
	}
	return lc.raw.Get(r)
}

type ListRecord struct {
	Time time.Time
	URI  string
	Mode string
}

func (lc ListingCombinator) update(r Reference, mode string) error {
	if list := lc.listReference.URI().String(); r.URI().String() == list {
		return fmt.Errorf("path conflict with listing: %s", list)
	}
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
	return lc.raw.Merge(lc.listReference, w.Bytes())
}

func (lc ListingCombinator) Put(r Reference, i interface{}) error {
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
