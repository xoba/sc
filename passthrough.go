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

func (pt Passthrough) Get(r Reference) (interface{}, error) {
	return pt.c.Get(r)
}

func (pt Passthrough) Put(r Reference, i interface{}) error {
	return pt.c.Put(r, i)
}

func (pt Passthrough) Merge(r Reference, i interface{}) error {
	return unimplemented(pt, "Merge")
}

func (pt Passthrough) Delete(r Reference) error {
	return pt.c.Delete(r)
}
