package sc

import (
	"fmt"
	"net/url"
	"os"
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

type FileSystem struct {
	mount string
}

func (fs FileSystem) Reference(string) (Reference, error) {
	return nil, fmt.Errorf("Reference unimplemented")
}
func (fs FileSystem) Get(Reference) (interface{}, error) {
	return nil, fmt.Errorf("Get unimplemented")
}
func (fs FileSystem) Put(Reference, interface{}) error {
	return fmt.Errorf("Put unimplemented")
}
func (fs FileSystem) Delete(Reference) error {
	return fmt.Errorf("Delete unimplemented")
}

func NewFileCombinator(mount string) (*FileSystem, error) {
	if _, err := os.Stat(mount); err != nil {
		return nil, err
	}
	return &FileSystem{mount: mount}, nil
}
