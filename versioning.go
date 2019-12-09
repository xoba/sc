package sc

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// usurps the uri fragment for versioning operations
func NewVersioning(c StorageCombinator) *Versioning {
	return &Versioning{
		c: c,
		p: regexp.MustCompile(`(versions)|(version)=([\d]+)`),
	}
}

// adds versioning to an existing combinator
type Versioning struct {
	c StorageCombinator
	p *regexp.Regexp
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
	e.SetEscapeHTML(false)
	e.Encode(r.URI().String())
	e.Encode(v)
	e.Encode(`D6871E1B-4C52-423B-B526-1F2D82D1C996`)
	return NewRef(fmt.Sprintf("%x", h.Sum(nil)))
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
	if err := v.checkReference(r); err != nil {
		return nil, err
	}
	r2, err := RemoveFragment(r)
	if err != nil {
		return nil, err
	}
	versions, err := v.load(r2)
	if err != nil {
		return nil, err
	}
	if u := r.URI(); v.p.MatchString(u.Fragment) {
		m := v.p.FindStringSubmatch(u.Fragment)
		switch {
		case m[1] == "versions":
			return versions, nil
		case m[2] == "version":
			x, err := strconv.ParseInt(m[3], 10, 64)
			if err != nil {
				return nil, err
			}
			vr, err := versions.Find(int(x))
			if err != nil {
				return nil, err
			}
			r, err := ParseRef(vr.TargetURI)
			if err != nil {
				return nil, err
			}
			return v.c.Get(r)
		default:
			return nil, fmt.Errorf("unrecognized uri fragment: %q", u.Fragment)
		}
	}
	if len(versions) == 0 {
		return nil, NotFound
	}
	// return the latest version
	r3, err := ParseRef(versions[len(versions)-1].TargetURI)
	if err != nil {
		return nil, err
	}
	return v.c.Get(r3)
}

func (v Versioning) checkReference(r Reference) error {
	u := r.URI()
	switch {
	case v.p.MatchString(u.Fragment):
		return nil
	case u.Fragment != "":
		return fmt.Errorf("versioning can't handle uri fragment %q", u.Fragment)
	default:
		return nil
	}
}

func (v Versioning) Put(r Reference, i interface{}) error {
	if err := v.checkReference(r); err != nil {
		return err
	}
	versions, err := v.load(r)
	if err != nil && !errors.Is(err, NotFound) {
		return err
	}
	newVersion := versions.Max() + 1
	targetURI := hashRef(r, newVersion)
	if err := v.c.Put(targetURI, i); err != nil {
		return err
	}
	versions = append(versions, VersionRecord{
		SourceURI: r.URI().String(),
		TargetURI: targetURI.URI().String(),
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

// need to think about this: should actually add a "delete" version,
// not delete it per se!!!
func (v Versioning) Delete(r Reference) error {
	if err := v.checkReference(r); err != nil {
		return err
	}
	return unimplemented(v, "Delete")

}

func (v Versioning) Merge(r Reference, i interface{}) error {
	if err := v.checkReference(r); err != nil {
		return err
	}
	return unimplemented(v, "Merge")
}
