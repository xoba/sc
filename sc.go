package sc

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
