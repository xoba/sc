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

func (m Multiplexer) find(r Reference) (StorageCombinator, error) {
	p := r.URI().Path
	first, err := firstPathComponent(p)
	if err != nil {
		return nil, err
	}
	c, ok := m.m[first]
	if !ok {
		return nil, fmt.Errorf("unsupported path: %q", p)
	}
	return c, nil
}

func (m Multiplexer) Get(r Reference) (interface{}, error) {
	c, err := m.find(r)
	if err != nil {
		return nil, err
	}
	return c.Get(r)
}

func (m Multiplexer) Put(r Reference, i interface{}) error {
	c, err := m.find(r)
	if err != nil {
		return err
	}
	return c.Put(r, i)
}

func (fs Multiplexer) Merge(r Reference, i interface{}) error {
	return unimplemented(fs, "Merge")
}

func (m Multiplexer) Delete(r Reference) error {
	c, err := m.find(r)
	if err != nil {
		return err
	}
	return c.Delete(r)
}
