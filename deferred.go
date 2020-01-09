package sc

import "sync"

type Deferred struct {
	factory func() (StorageCombinator, error)
	c       StorageCombinator
	lock    sync.Locker
}

func NewDeferred(factory func() (StorageCombinator, error)) Deferred {
	return Deferred{
		factory: factory,
		lock:    new(sync.Mutex),
	}
}

func (d *Deferred) Init() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.c != nil {
		return nil
	}
	c, err := d.factory()
	if err != nil {
		return err
	}
	d.c = c
	return nil
}

func (d *Deferred) Get(r Reference) (interface{}, error) {
	if err := d.Init(); err != nil {
		return nil, err
	}
	return d.c.Get(r)
}

func (d *Deferred) Put(r Reference, i interface{}) error {
	if err := d.Init(); err != nil {
		return err
	}
	return d.c.Put(r, i)
}

func (d *Deferred) Delete(r Reference) error {
	if err := d.Init(); err != nil {
		return err
	}
	return d.c.Delete(r)
}

func (d *Deferred) Merge(r Reference, i interface{}) error {
	if err := d.Init(); err != nil {
		return err
	}
	return d.c.Merge(r, i)
}
