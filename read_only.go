package sc

import "errors"

type ReadOnly struct {
	c StorageCombinator
}

func NewReadOnly(c StorageCombinator) *ReadOnly {
	return &ReadOnly{c: c}
}

var ReadOnlyError = errors.New("read only")

func (ro ReadOnly) Get(r Reference) (interface{}, error) {
	return ro.c.Get(r)
}

func (ro ReadOnly) Put(Reference, interface{}) error {
	return ReadOnlyError
}

func (ro ReadOnly) Delete(Reference) error {
	return ReadOnlyError
}

func (ro ReadOnly) Merge(Reference, interface{}) error {
	return ReadOnlyError
}
