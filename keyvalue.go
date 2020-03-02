package sc

type KeyValue interface {
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
}

type KeyValueCombinator struct {
	kv KeyValue
}

func NewKeyValue(kv KeyValue) *KeyValueCombinator {
	return &KeyValueCombinator{kv: kv}
}

func (c KeyValueCombinator) Get(r Reference) (interface{}, error) {
	return c.kv.Get(r.URI().String())
}

func (c KeyValueCombinator) Put(r Reference, i interface{}) error {
	return c.kv.Put(r.URI().String(), i)
}

func (c KeyValueCombinator) Delete(Reference) error {
	return unimplemented(c, "Delete")
}

func (c KeyValueCombinator) Merge(Reference, interface{}) error {
	return unimplemented(c, "Merge")
}
