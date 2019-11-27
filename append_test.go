package sc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestAppender(t *testing.T) {
	dir, err := ioutil.TempDir("", "sc_")
	check(err)
	defer os.RemoveAll(dir)
	fmt.Println(dir)
	ac, err := NewAppendingCombinator(dir, 0644)
	check(err)
	r1 := NewRef("a")
	const put = "howdy\n"
	check(ac.Put(r1, put))
	cmp := func(r Reference, str string) {
		x, err := ac.Get(r)
		check(err)
		var got string
		switch z := x.(type) {
		case []byte:
			got = string(z)
		case string:
			got = z
		default:
			t.Fatalf("bad return type: %T", t)
		}
		if got != str {
			t.Fatalf("got %q, expected %q\n", got, str)
		}
	}
	cmp(r1, put)
	r2 := NewRef("b")
	w := new(bytes.Buffer)
	for i := 0; i < 10; i++ {
		w.WriteString(put)
		check(ac.Merge(r2, put))
	}
	cmp(r2, w.String())
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
