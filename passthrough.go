package sc

import "log"

func NewPassthrough(msg string, c StorageCombinator) *Passthrough {
	return &Passthrough{
		c: c,
		m: msg,
	}
}

type Passthrough struct {
	c StorageCombinator
	m string
}

func (pt Passthrough) debug(msg string, r Reference) {
	if pt.m == "" {
		return
	}
	log.Printf("%s.%s: %v", pt.m, msg, r.URI())
}

func (pt Passthrough) Get(r Reference) (interface{}, error) {
	pt.debug("get", r)
	return pt.c.Get(r)
}

func (pt Passthrough) Put(r Reference, i interface{}) error {
	pt.debug("put", r)
	return pt.c.Put(r, i)
}

func (pt Passthrough) Merge(r Reference, i interface{}) error {
	pt.debug("merge", r)
	return pt.c.Merge(r, i)
}

func (pt Passthrough) Delete(r Reference) error {
	pt.debug("delete", r)
	return pt.c.Delete(r)
}

func (pt Passthrough) Find(p string) (Reference, error) {
	pt.debug("find", NewRef(p))
	return pt.c.Find(p)
}
