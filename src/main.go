package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/xoba/sc"
)

func main() {
	var store sc.StorageCombinator
	{
		fs, err := sc.NewFileCombinator("diskstore", os.ModePerm)
		check(err)
		store = fs
	}
	show := func(i interface{}) string {
		switch t := i.(type) {
		case []byte:
			return string(t)
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
	const dir = "subdir1/subdir2"
	for i := 0; i < 10; i++ {
		r, err := store.Reference("file:///" + path.Join(dir, fmt.Sprintf("test%d.txt", i)))
		check(err)
		check(store.Put(r, fmt.Sprintf("howdy %d!", i)))
		buf, err := store.Get(r)
		check(err)
		fmt.Printf("got %q\n", show(buf))
	}
	r2, err := store.Reference(dir)
	check(err)
	listing, err := store.Get(r2)
	check(err)
	fmt.Print(show(listing))
	check(Traverse(store, "../", 0))
}

func Traverse(store sc.StorageCombinator, p string, indent int) error {
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
				if err := Traverse(store, path.Join(p, x.Name), indent+1); err != nil {
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
