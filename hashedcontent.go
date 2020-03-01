package sc

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/crypto/sha3"
)

// enforces refs and content to be related by a hash
// references are of form hash://<algo>/<value>
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
	u.Scheme = HashURIScheme
	u.Host = h.algorithm
	u.Path = Base58Encode(h.value)
	return &u
}

func (h HashURI) String() string {
	return h.URI().String()
}

func NewHashURI(content []byte) (*HashURI, error) {
	hash, err := Hash(HashAlgo, content)
	if err != nil {
		return nil, err
	}
	return &HashURI{algorithm: HashAlgo, value: hash}, nil
}

func ParseHashRef(r Reference) (*HashURI, error) {
	u := r.URI()
	if u.Scheme != HashURIScheme {
		return nil, fmt.Errorf("unrecognized scheme %q", u.Scheme)
	}
	if u.Host != HashAlgo {
		return nil, fmt.Errorf("unrecognized algo %q", u.Host)
	}
	p := u.Path
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	dec, err := Base58Decode(p)
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

const (
	HashAlgo      = "shake256"
	HashURIScheme = "hash"
)

func Hash(algo string, buf []byte) ([]byte, error) {
	if algo != HashAlgo {
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
