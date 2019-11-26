package sc

func NewPassthrough(c StorageCombinator) *Passthrough {
	return &Passthrough{
		c: c,
	}
}

type Passthrough struct {
	c StorageCombinator
}

func (pt Passthrough) Get(r Reference) (interface{}, error) {
	return pt.c.Get(r)
}

func (pt Passthrough) Put(r Reference, i interface{}) error {
	return pt.c.Put(r, i)
}

func (pt Passthrough) Merge(r Reference, i interface{}) error {
	return pt.c.Merge(r, i)
}

func (pt Passthrough) Delete(r Reference) error {
	return pt.c.Delete(r)
}

func (pt Passthrough) Find(p string) (Reference, error) {
	return pt.c.Find(p)
}
