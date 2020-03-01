package sc

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/url"
)

type EncodedRefs struct {
	c StorageCombinator
}

func NewEncodedRefs(c StorageCombinator) EncodedRefs {
	return EncodedRefs{c: c}
}

func encode(r Reference) (Reference, error) {
	fmt.Printf("encoding %s\n", r)
	var u url.URL
	u.Scheme = "md5"
	h := md5.New()
	h.Write([]byte(r.URI().String()))
	u.Opaque = hex.EncodeToString(h.Sum(nil))
	return NewURI(&u), nil
}

func (e EncodedRefs) Get(r Reference) (interface{}, error) {
	er, err := encode(r)
	if err != nil {
		return nil, err
	}
	return e.c.Get(er)
}

func (e EncodedRefs) Put(r Reference, i interface{}) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Put(er, i)
}

func (e EncodedRefs) Delete(r Reference) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Delete(er)
}

func (e EncodedRefs) Merge(r Reference, i interface{}) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Merge(er, i)
}
