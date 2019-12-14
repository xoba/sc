package sc

import (
	"crypto/md5"
	"encoding/hex"
	"net/url"
)

type HashedRefs struct {
	c StorageCombinator
}

func NewHashedRefs(c StorageCombinator) HashedRefs {
	return HashedRefs{c: c}
}

// converts a reference into an opaque hashed reference
func hashedReference(r Reference) Reference {
	h := md5.New()
	h.Write([]byte(`EAE18B82-F047-4913-BFE7-CF5B9E3B35AB`))
	h.Write([]byte(r.URI().String()))
	var u url.URL
	u.Scheme = "md5"
	u.Opaque = hex.EncodeToString(h.Sum(nil))
	return NewURI(&u)
}

func (h HashedRefs) Get(r Reference) (interface{}, error) {
	return h.c.Get(hashedReference(r))
}

func (h HashedRefs) Put(r Reference, i interface{}) error {
	return h.c.Put(hashedReference(r), i)
}

func (h HashedRefs) Delete(r Reference) error {
	return h.c.Delete(hashedReference(r))
}

func (h HashedRefs) Merge(r Reference, i interface{}) error {
	return h.c.Merge(hashedReference(r), i)
}
