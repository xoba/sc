package sc

import "fmt"

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

func (m Multiplexer) find(r Reference) (StorageCombinator, error) {
	c, ok := m.m[r.Scheme]
	if !ok {
		return nil, fmt.Errorf("unsupported scheme: %q", r.Scheme)
	}
	return c, nil
}

func (m Multiplexer) Reference(r string) (Reference, error) {
	return Reference{Scheme: m.scheme, Path: r}, nil
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

func (m Multiplexer) Delete(r Reference) error {
	c, err := m.find(r)
	if err != nil {
		return err
	}
	return c.Delete(r)
}
