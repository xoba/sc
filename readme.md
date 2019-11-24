# storage combinators

playing around with storage combinators (weiher & hirschfeld). to get started
with an example, you could follow this recipe:
```
git clone git@github.com:xoba/sc.git
cd sc
export GOPATH=/tmp/gopath
go run src/main.go
```
or, to use this as a module in your own project, simply run:
```
go get github.com/xoba/sc
```
our interface for storage combinators is as follows, from [interface.go](https://github.com/xoba/sc/blob/master/interface.go):
```go
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
```
