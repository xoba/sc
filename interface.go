package sc

import (
	"errors"
	"net/url"
)

type StorageCombinator interface {
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
	Merge(Reference, interface{}) error
}

type Reference interface {
	URI() *url.URL
}

var (
	// if Get method cannot find reference, combinator should return something that wraps this error:
	NotFound = errors.New("reference not found")

	// if any method cannot be performed, combinator should return something that wraps this error:
	NotSupported = errors.New("operation not supported")
)
