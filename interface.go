package sc

import (
	"fmt"
	"net/url"
)

type StorageCombinator interface {
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Merge(Reference, interface{}) error
	Delete(Reference) error
}

type Reference interface {
	URI() url.URL
}

func unimplemented(i interface{}, method string) error {
	return fmt.Errorf("%T.%s unimplemented", i, method)
}
