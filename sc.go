package sc

import "encoding/json"

type StorageCombinator interface {
	Reference(string) (Reference, error) // gets a reference for a path
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
}

type Reference struct {
	Scheme string
	Path   string
}

func (r Reference) String() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}
