package sc

func NewCache(underlying, cache StorageCombinator) *Cache {
	return &Cache{
		u: underlying,
		c: cache,
	}
}

type Cache struct {
	u, c StorageCombinator
}

func (self Cache) Get(r Reference) (interface{}, error) {
	if i, err := self.c.Get(r); err == nil {
		return i, err
	}
	return self.u.Get(r)
}

func (self Cache) Find(p string) (Reference, error) {
	if r, err := self.c.Find(p); err == nil {
		return r, err
	}
	return self.u.Find(p)
}

func (self Cache) Put(r Reference, i interface{}) error {
	if err := self.c.Put(r, i); err != nil {
		return err
	}
	return self.u.Put(r, i)
}

func (self Cache) Merge(r Reference, i interface{}) error {
	if err := self.c.Merge(r, i); err != nil {
		return err
	}
	return self.u.Merge(r, i)
}

func (self Cache) Delete(r Reference) error {
	if err := self.c.Delete(r); err != nil {
		return err
	}
	return self.u.Delete(r)
}
