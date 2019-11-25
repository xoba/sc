package sc

import "net/url"

type StorageCombinator interface {
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
}

type Reference interface {
	URI() url.URL
}
