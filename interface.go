package sc

import (
	"net/url"
)

type StorageCombinator interface {
	Find(string) (Reference, error) // a sort of query or naming facility
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Merge(Reference, interface{}) error
	Delete(Reference) error
}

type Reference interface {
	URI() url.URL
}
