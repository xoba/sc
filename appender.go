package sc

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// will simply append data upon Merge call
type AppendingCombinator struct {
	dir  string
	mode os.FileMode
}

func NewAppendingCombinator(dir string, mode os.FileMode) (*AppendingCombinator, error) {
	if err := mkdir(dir, mode); err != nil {
		return nil, err
	}
	return &AppendingCombinator{
		dir:  dir,
		mode: mode,
	}, nil
}

func hash(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (ac AppendingCombinator) file(r Reference) string {
	return filepath.Join(ac.dir, hash(r.URI().Path))
}

func (ac AppendingCombinator) Find(p string) (Reference, error) {
	r := NewRef(p)
	r.u.Scheme = "merger"
	return r, nil
}

func (ac AppendingCombinator) Get(r Reference) (interface{}, error) {
	buf, err := ioutil.ReadFile(ac.file(r))
	if err != nil {
		return nil, wrapNotFound(r, err)
	}
	return buf, nil
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
	return ioutil.WriteFile(ac.file(r), buf, ac.mode)
}

func (ac AppendingCombinator) Delete(r Reference) error {
	return os.RemoveAll(ac.file(r))
}

// simple appends or creates
func (ac AppendingCombinator) Merge(r Reference, i interface{}) error {
	f, err := os.OpenFile(ac.file(r), os.O_APPEND|os.O_WRONLY|os.O_CREATE, ac.mode)
	if err != nil {
		return err
	}
	defer f.Close()
	var buf []byte
	switch t := i.(type) {
	case []byte:
		buf = t
	case string:
		buf = []byte(t)
	default:
		return fmt.Errorf("unsupported type: %T", t)
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	return nil
}
