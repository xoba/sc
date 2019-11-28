package sc

import (
	"fmt"
	"strings"
)

// NewMultiplexer creates a switching storage combinator,
// based on longest match to first path component
func NewMultiplexer(m map[string]StorageCombinator) (*Multiplexer, error) {
	return &Multiplexer{
		m: m,
	}, nil
}

type Multiplexer struct {
	m map[string]StorageCombinator
}

func firstPathComponent(p string) string {
	for _, p := range strings.Split(p, "/") {
		if len(p) == 0 {
			continue
		}
		return p
	}
	return ""
}

func (m Multiplexer) find(p string) (StorageCombinator, error) {
	first := firstPathComponent(p)
	var best string
	for k := range m.m {
		if !strings.HasPrefix(first, k) {
			continue
		}
		if len(k) > len(best) {
			best = k
		}
	}
	fmt.Printf("best match for %q: %q\n", p, best)
	c, ok := m.m[best]
	if !ok {
		return nil, fmt.Errorf("unsupported path: %q", p)
	}
	return c, nil
}

func (m Multiplexer) Get(r Reference) (interface{}, error) {
	c, err := m.find(r.URI().Path)
	if err != nil {
		return nil, err
	}
	return c.Get(r)
}

func (m Multiplexer) Put(r Reference, i interface{}) error {
	c, err := m.find(r.URI().Path)
	if err != nil {
		return err
	}
	return c.Put(r, i)
}

func (m Multiplexer) Merge(r Reference, i interface{}) error {
	c, err := m.find(r.URI().Path)
	if err != nil {
		return err
	}
	return c.Merge(r, i)
}

func (m Multiplexer) Delete(r Reference) error {
	c, err := m.find(r.URI().Path)
	if err != nil {
		return err
	}
	return c.Delete(r)
}

func (m Multiplexer) Find(p string) (Reference, error) {
	c, err := m.find(p)
	if err != nil {
		return nil, err
	}
	return c.Find(p)
}
