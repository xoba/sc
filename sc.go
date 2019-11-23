package sc

type StorageCombinator interface {
	Reference(string) (Reference, error)
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
}

type Reference interface {
	Scheme() string
	Path() string
}
