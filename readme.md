# storage combinators

playing around with storage combinators 
([weiher & hirschfeld](http://hirschfeld.org/writings/media/WeiherHirschfeld_2019_StorageCombinators_AcmDL_Preprint.pdf)). 
to get started with an example, you could follow this recipe:
```
git clone git@github.com:xoba/sc.git
cd sc
export GOPATH=/tmp/gopath
go run src/main.go -help
```
or, to use this as a module in your own project, simply run:
```
go get github.com/xoba/sc
```
our interface for storage combinators is as follows, from [interface.go](https://github.com/xoba/sc/blob/master/interface.go):
```go
type StorageCombinator interface {
	Get(Reference) (interface{}, error)
	Put(Reference, interface{}) error
	Delete(Reference) error
	Merge(Reference, interface{}) error
}

type Reference interface {
	URI() *url.URL
}
```
for using the s3 combinator, follow normal configuration conventions for using the aws sdk, such as having 
`~/.aws/credentials` and `~/.aws/config` files; e.g.:
```
[default]
aws_access_key_id = ********
aws_secret_access_key = ********
```
and
```
[default]
output = json
region = us-east-1
```
respectively.

please note that in major version 0, which is experimental, we do not offer
any compatibility guarantees.
