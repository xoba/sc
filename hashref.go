package sc

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/url"
)

type HashedRefs struct {
	c StorageCombinator
}

func hr(r Reference) Reference {
	h := md5.New()
	fmt.Fprint(h, r.URI().String())
	var u url.URL
	u.Scheme = "md5"
	u.Path = hex.EncodeToString(h.Sum(nil))
	return NewURI(&u)
}

func (h HashedRefs) Get(r Reference) (interface{}, error) {
	return h.c.Get(hr(r))
}

func (h HashedRefs) Put(r Reference, i interface{}) error {
	return h.c.Put(hr(r), i)
}

func (h HashedRefs) Delete(r Reference) error {
	return h.c.Delete(hr(r))
}

func (h HashedRefs) Merge(r Reference, i interface{}) error {
	return h.c.Merge(hr(r), i)
}
