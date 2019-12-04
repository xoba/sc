package sc

import (
	"bytes"
	"fmt"
	"io"
)

// adds an append merge function
type Appender struct {
	c StorageCombinator
}

func NewAppender(c StorageCombinator) (*Appender, error) {
	a := Appender{
		c: c,
	}
	return &a, nil
}

func (a Appender) Find(p string) (Reference, error) {
	return a.c.Find(p)
}

func (a Appender) Get(r Reference) (interface{}, error) {
	return a.c.Get(r)
}

func (a Appender) Put(r Reference, i interface{}) error {
	return a.c.Put(r, i)
}

func (a Appender) Delete(r Reference) error {
	return a.c.Delete(r)
}

func (a Appender) Merge(r Reference, i interface{}) error {
	original, err := a.c.Get(r)
	if err != nil {
		return err
	}
	w := new(bytes.Buffer)
	append := func(i interface{}) error {
		switch t := i.(type) {
		case []byte:
			if _, err := w.Write(t); err != nil {
				return err
			}
		case string:
			if _, err := w.WriteString(t); err != nil {
				return err
			}
		case io.Reader:
			if _, err := io.Copy(w, t); err != nil {
				return err
			}
		case io.ReadCloser:
			if _, err := io.Copy(w, t); err != nil {
				return err
			}
			if err := t.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled type: %T", t)
		}
		return nil
	}
	if err := append(original); err != nil {
		return err
	}
	if err := append(i); err != nil {
		return err
	}
	return a.c.Put(r, w.Bytes())
}
