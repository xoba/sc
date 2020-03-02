package sc

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"net/url"

	"golang.org/x/crypto/sha3"
)

const (
	DefaultHashAlgo = MD5

	MD5      = "md5"
	Shake256 = "shake256"
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

type HashReference struct {
	algorithm string
	value     []byte
}

func (h HashReference) URI() *url.URL {
	var u url.URL
	u.Scheme = h.algorithm
	u.Opaque = Base58Encode(h.value)
	return &u
}

func (h HashReference) String() string {
	return h.URI().String()
}

func NewHashReference(algo string, content []byte) (*HashReference, error) {
	hash, err := Hash(algo, content)
	if err != nil {
		return nil, err
	}
	return &HashReference{algorithm: algo, value: hash}, nil
}

func ParseHashRef(r Reference) (*HashReference, error) {
	u := r.URI()
	var decoded []byte
	switch u.Scheme {
	case Shake256, MD5:
		dec, err := Base58Decode(u.Opaque)
		if err != nil {
			return nil, err
		}
		decoded = dec
	default:
		return nil, fmt.Errorf("unrecognized algo %q", u.Scheme)
	}
	return &HashReference{algorithm: u.Scheme, value: decoded}, nil
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
	h1, err := NewHashReference(r.URI().Scheme, b)
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
	h1, err := NewHashReference(r.URI().Scheme, b)
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
	switch algo {
	case Shake256:
		h := sha3.NewShake256()
		if _, err := h.Write(buf); err != nil {
			return nil, err
		}
		out := make([]byte, 64)
		if _, err := h.Read(out); err != nil {
			return nil, err
		}
		return out, nil
	case MD5:
		h := md5.New()
		h.Write(buf)
		return h.Sum(nil), nil
	default:
		return nil, fmt.Errorf("hash algo %q not supported", algo)
	}
}
