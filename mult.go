package sc

import (
	"fmt"
	"strings"
)

// switching storage combinator, based on first path component
func NewMultiplexer(scheme string, m map[string]StorageCombinator) (*Multiplexer, error) {
	return &Multiplexer{
		scheme: scheme,
		m:      m,
	}, nil
}

type Multiplexer struct {
	scheme string
	m      map[string]StorageCombinator
}

func firstPathComponent(p string) (string, error) {
	for _, p := range strings.Split(p, "/") {
		if len(p) == 0 {
			continue
		}
		return p, nil
	}
	return "", fmt.Errorf("no first path component")
}

func (m Multiplexer) find(r *Reference) (StorageCombinator, *Reference, error) {
	if r.Scheme != m.scheme {
		return nil, nil, fmt.Errorf("bad scheme")
	}
	first, err := firstPathComponent(r.Path)
	if err != nil {
		return nil, nil, err
	}
	c, ok := m.m[first]
	if !ok {
		return nil, nil, fmt.Errorf("unsupported path: %q", r.Path)
	}
	r2, err := c.Reference(r.Path)
	if err != nil {
		return nil, nil, err
	}
	return c, r2, nil
}

func (m Multiplexer) Reference(r string) (*Reference, error) {
	return &Reference{Scheme: m.scheme, Path: r}, nil
}

func (m Multiplexer) Get(r *Reference) (interface{}, error) {
	c, r2, err := m.find(r)
	if err != nil {
		return nil, err
	}
	return c.Get(r2)
}

func (m Multiplexer) Put(r *Reference, i interface{}) error {
	c, r2, err := m.find(r)
	if err != nil {
		return err
	}
	return c.Put(r2, i)
}

func (m Multiplexer) Delete(r *Reference) error {
	c, r2, err := m.find(r)
	if err != nil {
		return err
	}
	return c.Delete(r2)
}
