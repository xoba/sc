package sc

import (
	"fmt"
	"net/url"
)

func unimplemented(i interface{}, method string) error {
	return fmt.Errorf("%T.%s unimplemented", i, method)
}

type Ref struct {
	u *url.URL
}

func (r Ref) String() string {
	return r.u.String()
}

func NewRef(p string) Ref {
	var r Ref
	r.u = &url.URL{}
	r.u.Path = p
	return r
}

func (r Ref) URI() *url.URL {
	return r.u
}
