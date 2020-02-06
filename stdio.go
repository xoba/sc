package sc

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// get/put vis stdin/stdout
type Stdio struct {
}

func (s Stdio) Get(r Reference) (interface{}, error) {
	w := new(bytes.Buffer)
	if _, err := io.Copy(w, os.Stdin); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (s Stdio) Put(r Reference, i interface{}) error {
	b, err := Blob(i)
	if err != nil {
		return err
	}
	n, err := os.Stdout.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("wrote %d / %d bytes", n, len(b))
	}
	return nil
}

func (s Stdio) Delete(Reference) error {
	return unimplemented(s, "Delete")
}

func (s Stdio) Merge(Reference, interface{}) error {
	return unimplemented(s, "Merge")
}
