package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"

	"github.com/xoba/sc"
)

var bucket string

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip")
	flag.Parse()
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

func main() {
	newFs := func() sc.StorageCombinator {
		fs, err := sc.NewFileSystem("file", "diskstore", os.ModePerm)
		check(err)
		return fs
	}
	var store sc.StorageCombinator
	if bucket == "" {
		store = newFs()
	} else {
		s3, err := sc.NewS3KeyValue("s3", bucket, "myprefix")
		check(err)
		m, err := sc.NewMultiplexer(map[string]sc.StorageCombinator{
			"dir0": newFs(),
			"dir1": s3,
		})
		check(err)
		store = m
	}
	store = sc.NewPassthrough("pass", store)
	for j := 0; j < 2; j++ {
		dir := fmt.Sprintf("/dir%d/sub", j)
		for i := 0; i < 3; i++ {
			r := NewRef(path.Join(dir, fmt.Sprintf("test%d.txt", i)))
			fmt.Println(r)
			check(store.Put(r, fmt.Sprintf("howdy %d!", i)))
			buf, err := store.Get(r)
			check(err)
			fmt.Printf("got %q\n", show(buf))
		}
	}
	r2 := NewRef("/dir0")
	listing, err := store.Get(r2)
	if err == nil {
		fmt.Print(show(listing))
	} else {
		fmt.Printf("can't get %s\n", r2)
	}
	if err := Traverse(store, "/"); err != nil {
		fmt.Printf("can't traverse '/'\n")
	}

	find := func(q string) {
		r, err := store.Find(q)
		if err != nil {
			fmt.Printf("can't find %q: %v\n", q, err)
		} else {
			fmt.Printf("found %q: %s\n", q, r.URI())
			o, err := store.Get(r)
			check(err)
			fmt.Printf("got: %q\n", show(o))
		}
	}
	find("dir0/sub/test0.txt")
	find("dir1/sub/test1.txt")

}

func show(i interface{}) string {
	switch t := i.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	case []sc.FileReference:
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		for _, x := range t {
			check(e.Encode(x))
		}
		return w.String()
	default:
		return fmt.Sprintf("%v\n", t)
	}
}

func Traverse(store sc.StorageCombinator, p string) error {
	return TraverseIndent(store, p, 0)
}

func TraverseIndent(store sc.StorageCombinator, p string, indent int) error {
	ref := NewRef(p)
	i, err := store.Get(ref)
	if err != nil {
		return err
	}
	var prefix string
	for i := 0; i < indent; i++ {
		prefix += "  "
	}
	switch t := i.(type) {
	case []sc.FileReference:
		for _, x := range t {
			fmt.Printf("%s%s (%d)\n", prefix, x.Name, x.Size)
			if x.IsDir {
				if err := TraverseIndent(store, path.Join(p, x.Name), indent+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
