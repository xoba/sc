package sc

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

func NewVersioning(c StorageCombinator) *Versioning {
	return &Versioning{c: c}
}

// adds versioning to an existing combinator
// a version query parameter
type Versioning struct {
	c StorageCombinator
}

type VersionRecord struct {
	SourceURI string
	TargetURI string
	Version   int
	Time      time.Time
}

type Versions []VersionRecord

func (v Versions) Max() (out int) {
	for i, x := range v {
		if i == 0 {
			out = x.Version
			continue
		}
		if x.Version > out {
			out = x.Version
		}
	}
	return
}

func hashRef(r Reference, v int) string {
	h := md5.New()
	e := json.NewEncoder(h)
	e.Encode(r)
	e.Encode(v)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (v Versioning) Find(p string) (Reference, error) {
	return v.c.Find(p)
}

func (v Versioning) load(r Reference) (Versions, error) {
	i, err := v.c.Get(r)
	if err != nil {
		return nil, err
	}
	var rd io.Reader
	switch t := i.(type) {
	case []byte:
		rd = bytes.NewReader(t)
	case string:
		rd = strings.NewReader(t)
	case io.ReadCloser:
		rd = t
	default:
		return nil, fmt.Errorf("unsupported type: %T", t)
	}
	var out Versions
	d := json.NewDecoder(rd)
	for {
		var v VersionRecord
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// uri fragment = "versions" returns the list of versions
func (v Versioning) Get(r Reference) (interface{}, error) {
	versions, err := v.load(r)
	if err != nil && !errors.Is(err, NotFound) {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, NotFound
	}
	if r.URI().Fragment == "versions" {
		return versions, nil
	}
	latest := versions[len(versions)-1]
	r2, err := ParseRef(latest.TargetURI)
	if err != nil {
		return nil, err
	}
	return v.c.Get(r2)
}

func (v Versioning) Put(r Reference, i interface{}) error {
	versions, err := v.load(r)
	if err != nil && !errors.Is(err, NotFound) {
		return err
	}
	newVersion := versions.Max() + 1
	target := NewRef(hashRef(r, newVersion))
	if err := v.c.Put(target, i); err != nil {
		return err
	}
	versions = append(versions, VersionRecord{
		SourceURI: r.URI().String(),
		TargetURI: target.URI().String(),
		Version:   newVersion,
		Time:      time.Now().UTC(),
	})
	w := new(bytes.Buffer)
	if err := versions.Encode(w); err != nil {
		return err
	}
	return v.c.Put(r, w.Bytes())
}

func (versions Versions) Encode(w io.Writer) error {
	e := json.NewEncoder(w)
	e.SetEscapeHTML(false)
	for _, x := range versions {
		if err := e.Encode(x); err != nil {
			return err
		}
	}
	return nil
}

func (v Versioning) Delete(r Reference) error {
	return v.c.Delete(r)
}

func (v Versioning) Merge(r Reference, i interface{}) error {
	return unimplemented(v, "Merge")
}
