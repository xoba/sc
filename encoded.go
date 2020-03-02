package sc

import "net/url"

type EncodedRefs struct {
	c StorageCombinator
}

func NewEncodedRefs(c StorageCombinator) EncodedRefs {
	return EncodedRefs{c: c}
}

func encode(r Reference) (Reference, error) {
	h, err := Hash(DefaultHashAlgo, []byte(r.URI().String()))
	if err != nil {
		return nil, err
	}
	var u url.URL
	u.Scheme = DefaultHashAlgo
	u.Opaque = Base58Encode(h)
	return NewURI(&u), nil
}

func (e EncodedRefs) Get(r Reference) (interface{}, error) {
	er, err := encode(r)
	if err != nil {
		return nil, err
	}
	return e.c.Get(er)
}

func (e EncodedRefs) Put(r Reference, i interface{}) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Put(er, i)
}

func (e EncodedRefs) Delete(r Reference) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Delete(er)
}

func (e EncodedRefs) Merge(r Reference, i interface{}) error {
	er, err := encode(r)
	if err != nil {
		return err
	}
	return e.c.Merge(er, i)
}
