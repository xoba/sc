package sc

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// will simply append data upon Merge call
type AppendingCombinator struct {
	dir string
}

func NewAppendingCombinator(dir string) (*AppendingCombinator, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	return &AppendingCombinator{
		dir: dir,
	}, nil
}

func hash(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (ac AppendingCombinator) file(r Reference) string {
	f := filepath.Join(ac.dir, hash(r.URI().Path))
	fmt.Println(f)
	return f
}

func (ac AppendingCombinator) Find(p string) (Reference, error) {
	r := NewRef(p)
	r.u.Scheme = "merger"
	return r, nil
}

func (ac AppendingCombinator) Get(r Reference) (interface{}, error) {
	return ioutil.ReadFile(ac.file(r))
}

func (ac AppendingCombinator) Put(r Reference, i interface{}) error {
	var buf []byte
	switch t := i.(type) {
	case []byte:
		buf = t
	case string:
		buf = []byte(t)
	default:
		return fmt.Errorf("unsupported type: %T", t)
	}
	return ioutil.WriteFile(ac.file(r), buf, os.ModePerm)
}

func (ac AppendingCombinator) Delete(r Reference) error {
	return os.RemoveAll(ac.file(r))
}

func (ac AppendingCombinator) Merge(r Reference, i interface{}) error {
	f := ac.file(r)
	w := new(bytes.Buffer)
	buf, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	w.Write(buf)
	switch t := i.(type) {
	case []byte:
		w.Write(t)
	case string:
		w.WriteString(t)
	default:
		return fmt.Errorf("unsupported type: %T", t)
	}
	return ioutil.WriteFile(f, w.Bytes(), os.ModePerm)
}
