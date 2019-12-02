package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blang/semver"
	"github.com/xoba/sc"
)

var bucket string
var retag bool

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip s3 altogether if blank")
	flag.BoolVar(&retag, "tag", false, "re-tag with next patch version")
	flag.Parse()
}

func runCmd(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	w := new(bytes.Buffer)
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func RunRetag() error {
	buf, err := runCmd("git", "tag")
	if err != nil {
		return err
	}
	var list []semver.Version
	s := bufio.NewScanner(bytes.NewReader(buf))
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "v") {
			return fmt.Errorf("bad tag: %q", line)
		}
		line = line[1:]
		v, err := semver.Make(line)
		if err != nil {
			return err
		}
		list = append(list, v)
	}
	if err := s.Err(); err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].LT(list[j])
	})
	for _, v := range list {
		fmt.Println(v)
	}
	next := list[len(list)-1]
	next.Patch += 1
	fmt.Printf("next = %v\n", next)
	if _, err := runCmd("git", "tag", "v"+next.String()); err != nil {
		return err
	}
	if _, err := runCmd("git", "push", "--tag"); err != nil {
		return err
	}
	return nil
}

func newFilesystem(mount string) sc.StorageCombinator {
	c, err := sc.NewFileSystem("", mount, os.ModePerm)
	check(err)
	return c
}

func newS3(bucket, prefix string) sc.StorageCombinator {
	p, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	check(err)
	c, err := sc.NewS3KeyValue(bucket, prefix, true, s3.New(p))
	check(err)
	return c
}

func newAppender(dir string) sc.StorageCombinator {
	check(os.MkdirAll(dir, os.ModePerm))
	c, err := sc.NewAppendingCombinator(dir, 0644)
	check(err)
	return c
}

func newMultiplexer(m map[string]sc.StorageCombinator) sc.StorageCombinator {
	c, err := sc.NewMultiplexer(m)
	check(err)
	return c
}

func newLister(raw, appender sc.StorageCombinator, path string) sc.StorageCombinator {
	c, err := sc.NewListingCombinator(raw, appender, path)
	check(err)
	return c
}

func main() {

	if retag {
		check(RunRetag())
	}

	const (
		workingDir = "diskstore/work"
		cacheDir   = "diskstore/cache"
		merger     = "diskstore/merger"
		listPath   = "/list"
		prefix     = "myprefix"
	)

	// our top-level storage combinator, which will be assembled from parts:
	var store sc.StorageCombinator

	{
		store = sc.NewVersioning(newFilesystem(workingDir))
		store = newLister(store, newAppender(merger), listPath)
		store = sc.NewPassthrough("v", store)
		r := sc.NewRef("test.txt")
		check(store.Put(r, fmt.Sprintf("howdy at %v!", time.Now())))
		i, err := store.Get(r)
		check(err)
		fmt.Printf("got: %s\n", show(i))
		u, err := sc.ParseRef("test.txt#versions")
		check(err)
		list, err := store.Get(u)
		check(err)
		fmt.Printf("list:\n%s\n", show(list))
		for _, v := range list.(sc.Versions) {
			r, err := sc.ParseRef(fmt.Sprintf("test.txt?version=%d#versions", v.Version))
			check(err)
			i, err := store.Get(r)
			check(err)
			fmt.Printf("%s: %s\n", r, show(i))
		}
		return
	}

	// create either a pure disk filesystem or file+s3 multiplexed:
	{
		fs := sc.NewPassthrough("fs", newFilesystem(workingDir))
		calculator := sc.NewProgrammatic(func(r sc.Reference) (interface{}, error) {
			u := r.URI()
			if q := u.Query(); len(q) > 0 {
				return fmt.Sprintf("got sql query %q", q.Get("sql")), nil
			}
			var count int
			for range u.String() {
				count++
			}
			return fmt.Sprintf("%q has %d chars", u, count), nil
		})
		m := map[string]sc.StorageCombinator{
			"":     fs,
			"/":    fs,
			"dir0": fs,
			"calc": calculator,
		}
		if bucket != "" {
			m["dir1"] = newS3(bucket, prefix)
		}
		store = newMultiplexer(m)
	}

	// add listing capability:
	store = newLister(store, newAppender(merger), listPath)

	store = sc.NewPassthrough("cache", sc.NewCache(store, newFilesystem(cacheDir)))

	// a passthrough for fun:
	store = sc.NewPassthrough("", store)

	// test out the calculator:
	for i := 0; i < 3; i++ {
		r := sc.NewRef(fmt.Sprintf("/calc/%f", math.Pow(10, float64(i))))
		i, err := store.Get(r)
		check(err)
		fmt.Println(show(i))
	}

	// play with a sql query concept:
	{
		fmt.Printf("gonna try a sql query\n")
		u, err := url.Parse("/calc?sql=select * from mytable")
		check(err)
		i, err := store.Get(sc.NewURI(u))
		check(err)
		fmt.Println(show(i))
	}

	// put a bunch of stuff at various paths, and see if we can retrieve it
	for j := 0; j < 2; j++ {
		dir := fmt.Sprintf("/dir%d/sub", j)
		for i := 0; i < 3; i++ {
			r := sc.NewRef(path.Join(dir, fmt.Sprintf("test_%d_%d.txt", i, j)))
			fmt.Println(r)
			check(store.Put(r, fmt.Sprintf("howdy %d/%d!", i, j)))
			buf, err := store.Get(r)
			check(err)
			fmt.Printf("got %q\n", show(buf))
		}
	}

	// see if we can get a directory from filesystem combinator:
	{
		r := sc.NewRef("/dir0")
		listing, err := store.Get(r)
		if err == nil {
			fmt.Print(show(listing))
		} else {
			fmt.Printf("can't get directory %s\n", r)
		}
	}

	// see if we can traverse disk part of filesystem
	const root = "/"
	fmt.Printf("traversing %q as best we can\n", root)
	if err := Traverse(store, root); err != nil {
		fmt.Printf("can't traverse %q': %v\n", root, err)
	}

	// a function to find stuff with our combinator and show it
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

	find("/dir0/sub/test_0_0.txt")
	find("/dir1/sub/test_1_1.txt")

	// use the lister functionality to get a list of what we mutated
	{
		list, err := store.Find(listPath)
		check(err)
		x, err := store.Get(list)
		check(err)
		fmt.Println(show(x))
	}
}

// interprets and shows something we get from a storage combinator
func show(i interface{}) string {
	switch t := i.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	case io.ReadCloser:
		defer t.Close()
		w := new(bytes.Buffer)
		if _, err := io.Copy(w, t); err != nil {
			check(err)
		}
		return w.String()
	case []sc.FileReference:
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		for _, x := range t {
			check(e.Encode(x))
		}
		return w.String()
	case sc.Versions:
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		for _, v := range t {
			check(e.Encode(v))
		}
		return w.String()
	default:
		return fmt.Sprintf("%v\n", t)
	}
}

func Traverse(store sc.StorageCombinator, p string) error {
	return TraverseIndent(store, p, 0)
}

// if what we get from a combinator is a directory, like from file system, list and traverse it
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
	case sc.Directory:
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
