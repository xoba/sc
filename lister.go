package sc

import (
	"encoding/json"
	"net/url"
)

type ListingCombinator struct {
	raw, list     StorageCombinator
	dir           string
	listReference Reference
}

func NewListingCombinator(raw StorageCombinator, listingDir string) (*ListingCombinator, error) {
	list, err := NewAppendingCombinator(listingDir)
	if err != nil {
		return nil, err
	}
	r, err := list.Find("listing.txt")
	if err != nil {
		return nil, err
	}
	if err := list.Put(r, ""); err != nil {
		return nil, err
	}
	return &ListingCombinator{
		raw:           raw,
		list:          list,
		listReference: r,
		dir:           listingDir,
	}, nil
}

func (lc ListingCombinator) Find(p string) (Reference, error) {
	if p == "/"+lc.dir {

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
	URI    *url.URL
	Delete bool `json:",omitempty"`
}

func (lc ListingCombinator) update(r Reference, delete bool) error {
	lr := ListRecord{
		URI:    r.URI(),
		Delete: delete,
	}
	buf, err := json.Marshal(lr)
	if err != nil {
		return err
	}
	return lc.list.Put(lc.listReference, buf)
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
