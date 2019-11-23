package main

import (
	"fmt"
	"os"
	"xoba/sc"
)

func main() {
	const dir = "diskstore"
	check(os.MkdirAll(dir, os.ModePerm))
	var store sc.StorageCombinator
	fs, err := sc.NewFileCombinator(dir)
	check(err)
	fmt.Println(fs)
	store = fs
	r, err := store.Reference("test.txt")
	check(err)
	fmt.Println(r)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
