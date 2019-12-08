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
	if err := self.c.Delete(r); err != nil {
		return err
	}
	if err := self.u.Put(r, i); err != nil {
		return err
	}
	tmpCopy, err := self.u.Get(r)
	if err != nil {
		return err
	}
	return self.c.Put(r, tmpCopy)
}

func (self Cache) Merge(r Reference, i interface{}) error {
	if err := self.c.Delete(r); err != nil {
		return err
	}
	if err := self.u.Merge(r, i); err != nil {
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
