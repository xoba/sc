package sc

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

func NewVersioning(c StorageCombinator) *Versioning {
	return &Versioning{c: c}
}

// adds versioning to an existing combinator
type Versioning struct {
	c StorageCombinator
}

type VersionRecord struct {
	SourceURI string
	TargetURI string
	Version   int
	Time      time.Time
}

// assumed to be sorted in ascending version order
type Versions []VersionRecord

func (v Versions) Find(version int) (*VersionRecord, error) {
	i := sort.Search(len(v), func(i int) bool {
		return v[i].Version >= version
	})
	if i < len(v) && v[i].Version == version {
		return &v[i], nil
	} else {
		return nil, NotFound
	}
}

func (v Versions) Max() (out int) {
	n := len(v)
	if n == 0 {
		return 0
	}
	return v[n-1].Version

}

func hashRef(r Reference, v int) Reference {
	h := md5.New()
	e := json.NewEncoder(h)
	e.Encode(r)
	e.Encode(v)
	return NewRef(fmt.Sprintf("%x", h.Sum(nil)))
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
// unless there is also a "version" query parameter, then
// that version is retrieved
func (v Versioning) Get(r Reference) (interface{}, error) {
	versions, err := v.load(r)
	if err != nil && !errors.Is(err, NotFound) {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, NotFound
	}
	versionIndex := func(i int) (interface{}, error) {
		vr, err := versions.Find(i)
		if err != nil {
			return nil, err
		}
		r, err := ParseRef(vr.TargetURI)
		if err != nil {
			return nil, err
		}
		return v.c.Get(r)
	}
	if r.URI().Fragment != "versions" {
		return versionIndex(len(versions) - 1)
	}
	q := r.URI().Query()
	if version := q.Get("version"); version != "" {
		x, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return nil, err
		}
		return versionIndex(int(x))
	}
	return versions, nil
}

func (v Versioning) Put(r Reference, i interface{}) error {
	versions, err := v.load(r)
	if err != nil && !errors.Is(err, NotFound) {
		return err
	}
	newVersion := versions.Max() + 1
	target := hashRef(r, newVersion)
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
