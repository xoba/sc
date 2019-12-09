package sc

func NewCache(underlying, cache StorageCombinator) *Cache {
	return &Cache{
		u: underlying,
		c: cache,
	}
}

type Cache struct {
	u, c, tmp StorageCombinator
}

func (self Cache) Get(r Reference) (interface{}, error) {
	if i, err := self.c.Get(r); err == nil {
		return i, err
	}
	if i, err := self.u.Get(r); err != nil {
		return nil, err
	} else if err := self.c.Put(r, i); err != nil {
		return nil, err
	}
	return self.Get(r)
}

func (self Cache) Put(r Reference, i interface{}) error {
	return self.update(r, i, self.u.Put)
}

// merges with underlying, but puts merged version to cache
func (self Cache) Merge(r Reference, i interface{}) error {
	return self.update(r, i, self.u.Merge)
}

func (self Cache) update(r Reference, i interface{}, mutator func(Reference, interface{}) error) error {
	if err := self.c.Delete(r); err != nil {
		return err
	}
	if err := mutator(r, i); err != nil {
		return err
	}
	tmpCopy, err := self.u.Get(r)
	if err != nil {
		return err
	}
	return self.c.Put(r, tmpCopy)
}

func (self Cache) Delete(r Reference) error {
	if err := self.c.Delete(r); err != nil {
		return err
	}
	return self.u.Delete(r)
}
