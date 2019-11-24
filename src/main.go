package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/xoba/sc"
)

var bucket string

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip")
	flag.Parse()
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
		buf, err := ioutil.ReadFile("bucket.txt")
		check(err)
		s3, err := sc.NewS3KeyValue("s3", strings.TrimSpace(string(buf)), "myprefix")
		check(err)
		m, err := sc.NewMultiplexer("mult", map[string]sc.StorageCombinator{
			"dir0": newFs(),
			"dir1": s3,
		})
		check(err)
		store = m
	}
	store = sc.NewPassthrough("pass", store)
	for j := 0; j < 2; j++ {
		dir := fmt.Sprintf("/dir%d/sub", j)
		for i := 0; i < 10; i++ {
			r, err := store.Reference(path.Join(dir, fmt.Sprintf("test%d.txt", i)))
			check(err)
			fmt.Println(r)
			check(store.Put(r, fmt.Sprintf("howdy %d!", i)))
			buf, err := store.Get(r)
			check(err)
			fmt.Printf("got %q\n", show(buf))
		}
	}
	r2, err := store.Reference("/dir0")
	check(err)
	listing, err := store.Get(r2)
	if err == nil {
		fmt.Print(show(listing))
	} else {
		fmt.Printf("can't get %s\n", r2)
	}
	if err := Traverse(store, "/"); err != nil {
		fmt.Printf("can't traverse '/'\n")
	}
}

func show(i interface{}) string {
	switch t := i.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	case []sc.File:
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
	ref, err := store.Reference(p)
	if err != nil {
		return err
	}
	i, err := store.Get(ref)
	if err != nil {
		return err
	}
	var prefix string
	for i := 0; i < indent; i++ {
		prefix += "  "
	}
	switch t := i.(type) {
	case []sc.File:
		for _, x := range t {
			fmt.Printf("%s%s (%d)\n", prefix, x.Name, x.Size)
			if x.IsDir {
				if err := TraverseIndent(store, path.Join(p, x.Name), indent+1); err != nil {
					return err
				}
			}
		}
	default:
	}
	return nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
