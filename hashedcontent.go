package sc

import (
	"bytes"
	"fmt"
	"net/url"

	"golang.org/x/crypto/sha3"
)

const (
	DefaultHashAlgo = "shake256"
)

// enforces refs and content to be related by a hash.
// references are of form <algo>:<value>
// where <algo> is name of algorithm,
// <value> is base58-encoded value of hash
type HashedContent struct {
	c StorageCombinator
}

func NewHashedContent(c StorageCombinator) HashedContent {
	return HashedContent{c: c}
}

type HashURI struct {
	algorithm string
	value     []byte
}

func (h HashURI) URI() *url.URL {
	var u url.URL
	u.Scheme = DefaultHashAlgo
	u.Opaque = Base58Encode(h.value)
	return &u
}

func (h HashURI) String() string {
	return h.URI().String()
}

func NewHashURI(content []byte) (*HashURI, error) {
	hash, err := Hash(DefaultHashAlgo, content)
	if err != nil {
		return nil, err
	}
	return &HashURI{algorithm: DefaultHashAlgo, value: hash}, nil
}

func ParseHashRef(r Reference) (*HashURI, error) {
	u := r.URI()
	if u.Scheme != DefaultHashAlgo {
		return nil, fmt.Errorf("unrecognized algo %q", u.Scheme)
	}
	dec, err := Base58Decode(u.Opaque)
	if err != nil {
		return nil, err
	}
	return &HashURI{algorithm: u.Host, value: dec}, nil
}

func (hc HashedContent) Get(r Reference) (interface{}, error) {
	h0, err := ParseHashRef(r)
	if err != nil {
		return nil, err
	}
	i, err := hc.c.Get(r)
	if err != nil {
		return nil, err
	}
	b, err := Blob(i)
	if err != nil {
		return nil, err
	}
	h1, err := NewHashURI(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(h0.value, h1.value) != 0 {
		return nil, fmt.Errorf("hashes disagree")
	}
	return b, nil
}

func (hc HashedContent) Put(r Reference, i interface{}) error {
	h0, err := ParseHashRef(r)
	if err != nil {
		return err
	}
	b, err := Blob(i)
	if err != nil {
		return err
	}
	h1, err := NewHashURI(b)
	if err != nil {
		return err
	}
	if bytes.Compare(h0.value, h1.value) != 0 {
		return fmt.Errorf("hashes disagree")
	}
	return hc.c.Put(r, b)
}

func (hc HashedContent) Delete(r Reference) error {
	return unimplemented(hc, "Delete")
}

func (hc HashedContent) Merge(r Reference, i interface{}) error {
	return unimplemented(hc, "Merge")
}

func Hash(algo string, buf []byte) ([]byte, error) {
	if algo != DefaultHashAlgo {
		return nil, fmt.Errorf("hash algo %q not supported", algo)
	}
	h := sha3.NewShake256()
	if _, err := h.Write(buf); err != nil {
		return nil, err
	}
	out := make([]byte, 64)
	if _, err := h.Read(out); err != nil {
		return nil, err
	}
	return out, nil
}
