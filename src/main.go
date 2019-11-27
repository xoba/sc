package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/xoba/sc"
)

var bucket string

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip")
	flag.Parse()
}

func main() {

	if false {
		r := sc.NewRef("test.txt")
		const dir = "merging"
		os.MkdirAll(dir, os.ModePerm)
		ac, err := sc.NewAppendingCombinator(dir, 0644)
		check(err)
		check(ac.Put(r, "first line\n"))
		for i := 0; i < 10; i++ {
			check(ac.Merge(r, fmt.Sprintf("howdy %d!\n", i)))
		}
		return

	}

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

	store = sc.NewPassthrough(store)

	const listPath = "/list"
	{
		const dir = "diskstore/merging"
		os.MkdirAll(dir, os.ModePerm)
		ac, err := sc.NewAppendingCombinator(dir, 0644)
		check(err)
		lister, err := sc.NewListingCombinator(store, ac, listPath)
		check(err)
		store = lister
	}

	for j := 0; j < 2; j++ {
		dir := fmt.Sprintf("/dir%d/sub", j)
		for i := 0; i < 3; i++ {
			r := sc.NewRef(path.Join(dir, fmt.Sprintf("test%d.txt", i)))
			fmt.Println(r)
			check(store.Put(r, fmt.Sprintf("howdy %d!", i)))
			buf, err := store.Get(r)
			check(err)
			fmt.Printf("got %q\n", show(buf))
		}
	}
	r2 := sc.NewRef("/dir0")
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

	list, err := store.Find(listPath)
	check(err)
	x, err := store.Get(list)
	check(err)
	fmt.Println(show(x))
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
	ref := sc.NewRef(p)
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
