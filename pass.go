package sc

func NewPassthrough(scheme string, c StorageCombinator) *Passthrough {
	return &Passthrough{
		scheme: scheme,
		c:      c,
	}
}

type Passthrough struct {
	scheme string
	c      StorageCombinator
}

func (pt Passthrough) Reference(p string) (*Reference, error) {
	return &Reference{Scheme: pt.scheme, Path: p}, nil
}

func (pt Passthrough) Get(r *Reference) (interface{}, error) {
	r2, err := pt.c.Reference(r.Path)
	if err != nil {
		return nil, err
	}
	return pt.c.Get(r2)
}

func (pt Passthrough) Put(r *Reference, i interface{}) error {
	r2, err := pt.c.Reference(r.Path)
	if err != nil {
		return err
	}
	return pt.c.Put(r2, i)
}

func (pt Passthrough) Delete(r *Reference) error {
	r2, err := pt.c.Reference(r.Path)
	if err != nil {
		return err
	}
	return pt.c.Delete(r2)
}
