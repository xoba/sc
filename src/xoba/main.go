package main

import (
	"fmt"
	"os"
	"path"
	"xoba/sc"
)

func main() {
	var store sc.StorageCombinator
	{
		fs, err := sc.NewFileCombinator("diskstore", os.ModePerm)
		check(err)
		store = fs
	}
	const dir = "subdir1/subdir2/"
	show := func(i interface{}) string {
		switch t := i.(type) {
		case []byte:
			return string(t)
		default:
			return fmt.Sprintf("%v\n", t)
		}
	}
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
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
