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

type (
	f0 func(Reference) error
	f1 func(Reference) (interface{}, error)
	f2 func(Reference, interface{}) error
	f3 func(string) (Reference, error)
)

func (pt Passthrough) debug0(m string, f f0, r Reference) error {
	err := f(r)
	if pt.m != "" {
		log.Printf("%s.%s(%s) = %v", pt.m, m, r.URI(), err)
	}
	return err
}

func (pt Passthrough) debug1(m string, f f1, r Reference) (interface{}, error) {
	i, err := f(r)
	if pt.m != "" {
		log.Printf("%s.%s(%s) = (%T,%v)", pt.m, m, r.URI(), i, err)
	}
	return i, err
}

func (pt Passthrough) debug2(m string, f f2, r Reference, i interface{}) error {
	err := f(r, i)
	if pt.m != "" {
		log.Printf("%s.%s(%s,%T) = %v", pt.m, m, r.URI(), i, err)
	}
	return err
}

func (pt Passthrough) debug3(m string, f f3, p string) (Reference, error) {
	r, err := f(p)
	if pt.m != "" {
		log.Printf("%s.%s(%s) = (%s,%v)", pt.m, m, p, r.URI(), err)
	}
	return r, err
}

func (pt Passthrough) Get(r Reference) (interface{}, error) {
	return pt.debug1("Get", pt.c.Get, r)
}

func (pt Passthrough) Put(r Reference, i interface{}) error {
	return pt.debug2("Put", pt.c.Put, r, i)
}

func (pt Passthrough) Merge(r Reference, i interface{}) error {
	return pt.debug2("Merge", pt.c.Merge, r, i)
}

func (pt Passthrough) Delete(r Reference) error {
	return pt.debug0("Delete", pt.c.Delete, r)
}

func (pt Passthrough) Find(p string) (Reference, error) {
	return pt.debug3("Find", pt.c.Find, p)
}
