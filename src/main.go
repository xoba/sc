package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/xoba/sc"
)

var bucket string

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip s3 altogether if blank")
	flag.Parse()
}

func newFilesystem(mount string) sc.StorageCombinator {
	fs, err := sc.NewFileSystem("", mount, os.ModePerm)
	check(err)
	return fs
}

func newS3(bucket, prefix string) sc.StorageCombinator {
	fs, err := sc.NewS3KeyValue("", bucket, prefix)
	check(err)
	return fs
}

func newAppender(dir string) sc.StorageCombinator {
	os.MkdirAll(dir, os.ModePerm)
	ac, err := sc.NewAppendingCombinator(dir, 0644)
	check(err)
	return ac
}

func newMultiplexer(m map[string]sc.StorageCombinator) sc.StorageCombinator {
	x, err := sc.NewMultiplexer(m)
	check(err)
	return x
}

func newLister(path string, appender, raw sc.StorageCombinator) sc.StorageCombinator {
	lister, err := sc.NewListingCombinator(raw, appender, path)
	check(err)
	return lister
}

func main() {

	const (
		workingDir = "diskstore"
		listPath   = "/list"
		prefix     = "myprefix"
	)

	var store sc.StorageCombinator
	{
		fs := newFilesystem(workingDir)
		if bucket == "" {
			store = fs
		} else {
			store = newMultiplexer(map[string]sc.StorageCombinator{
				"dir0": fs,
				"dir1": newS3(bucket, prefix),
			})
		}
	}

	store = sc.NewPassthrough(store)

	store = newLister(listPath, newAppender(filepath.Join(workingDir, "merging")), store)

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

	{
		r := sc.NewRef("/dir0")
		listing, err := store.Get(r)
		if err == nil {
			fmt.Print(show(listing))
		} else {
			fmt.Printf("can't get listing %s\n", r)
		}
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

	{
		list, err := store.Find(listPath)
		check(err)
		x, err := store.Get(list)
		check(err)
		fmt.Println(show(x))
	}
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
