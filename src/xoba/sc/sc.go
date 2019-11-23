package sc

import (
	"net/url"
)

type StorageCombinator interface {
	Reference(string) (Reference, error)
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
}

type Reference interface {
	URI() url.URL
}
