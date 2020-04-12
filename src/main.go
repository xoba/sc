package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blang/semver"
	"github.com/google/uuid"
	_ "github.com/snowflakedb/gosnowflake"
	"github.com/xoba/sc"
)

var bucket, user, host, passwordVar, ftpFile string
var retag bool

func init() {
	flag.StringVar(&bucket, "b", "", "bucket to use for s3, or skip s3 altogether if blank")
	flag.StringVar(&host, "h", "", "ftp host")
	flag.StringVar(&user, "u", "", "ftp user")
	flag.StringVar(&ftpFile, "f", "", "the ftp file to Get")
	flag.StringVar(&passwordVar, "p", "FTP_PASSWORD", "ftp password environment variable")
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

type Version struct {
	v semver.Version
}

func (v Version) String() string {
	return fmt.Sprintf("v%s", v.v)
}

func RunRetag() error {
	if buf, err := runCmd("git", "status", "--porcelain"); err != nil {
		return err
	} else if len(buf) > 0 {
		return fmt.Errorf("can't tag with unclean status")
	}
	buf, err := runCmd("git", "tag")
	if err != nil {
		return err
	}
	var list []Version
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
		list = append(list, Version{v: v})
	}
	if err := s.Err(); err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].v.LT(list[j].v)
	})
	next := list[len(list)-1]
	next.v.Patch += 1
	if true {
		next.v.Pre = nil
	} else {
		pr, err := semver.NewPRVersion(time.Now().UTC().Format("20060102T150405Z"))
		check(err)
		next.v.Pre = []semver.PRVersion{pr}
	}
	fmt.Printf("next = %v\n", next)
	if _, err := runCmd("git", "tag", next.String()); err != nil {
		return err
	}
	if _, err := runCmd("git", "push", "--tag"); err != nil {
		return err
	}
	return nil
}

func newFilesystem(mount string) sc.StorageCombinator {
	c, err := sc.NewFileSystem(mount)
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

func encrypt(c sc.StorageCombinator) sc.StorageCombinator {
	p, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	check(err)
	keyID, err := LoadKeyInfo()
	check(err)
	e, err := sc.NewEncrypter(kms.New(p), keyID, c)
	check(err)
	return e
}

// kms.txt contains key id
func LoadKeyInfo() (keyID string, err error) {
	buf, err := ioutil.ReadFile("kms.txt")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buf)), nil
}

func newAppender(dir string) sc.StorageCombinator {
	c, err := sc.NewFileAppender(dir)
	check(err)
	return c
}

func NewStorageCombinator(base string, listRef sc.Reference) (sc.StorageCombinator, error) {
	log := func(c sc.StorageCombinator) sc.StorageCombinator {
		return sc.NewPassthrough(fmt.Sprintf("%T", c), c)
	}
	store := log(
		sc.NewListingCombinator(
			log(
				sc.NewVersioning(
					log(
						newFilesystem(
							path.Join(base, "fs"),
						),
					),
				),
			),
			listRef,
		),
	)
	return store, nil
}

func appenderTest() {
	c := sc.NewAppender(newFilesystem("diskstore"))
	check(c.Put(sc.NewRef("a.txt"), "hi there A\n"))
	check(c.Put(sc.NewRef("b.txt"), "hi there B\n"))
	check(c.Put(sc.NewRef("c.txt"), "hi there C\n"))
	check(c.Merge(sc.NewRef("a.txt"), "hello again!\n"))
}

const (
	workingDir = "diskstore/work"
	cacheDir   = "diskstore/cache"
	merger     = "diskstore/merger"
	listPath   = "/list"
	prefix     = "myprefix"
)

func OpenDB() (*sql.DB, error) {
	m, err := dbInfo()
	if err != nil {
		return nil, err
	}
	u, err := m.URL()
	if err != nil {
		return nil, err
	}
	return sql.Open("snowflake", u.String())
}

type DBInfo struct {
	Account   string
	User      string
	Password  string
	Query     string
	DB        string
	Schema    string
	Warehouse string
}

func (m DBInfo) URL() (*url.URL, error) {
	return url.Parse(fmt.Sprintf("%[1]s:%[2]s@%[3]s/%[4]s/%[5]s?warehouse=%[6]s",
		url.QueryEscape(m.User),
		url.QueryEscape(m.Password),
		m.Account,
		m.DB,
		m.Schema,
		m.Warehouse,
	))
}

type APIInfo struct {
	Endpoint string
	Key      string
}

func dbInfo() (*DBInfo, error) {
	f, err := os.Open("snowflake.json")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	var m DBInfo
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func testhashes() {
	var c sc.StorageCombinator
	{
		fs, err := sc.NewFileSystem("diskstore")
		check(err)
		c = fs
	}
	c = sc.NewHashedContent(c)
	blob := []byte(fmt.Sprintf("test 123 at %s\n", time.Now()))
	fmt.Printf("going to store:\n%q\n", string(blob))
	r, err := sc.Hash(sc.DefaultHashAlgo, blob)
	check(err)
	fmt.Println(r)
	check(c.Put(r, blob))

	i, err := c.Get(r)
	check(err)
	fmt.Printf("got:\n%q\n", show(i))
}

func main() {

	if retag {
		check(RunRetag())
		return
	}

	if true {
		p, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		check(err)
		check(sc.DeleteKeys(s3.New(p), bucket, []string{"5766FA40-0622-457E-A898-F6EA68061388"}))
		return
	}

	if true {
		testhashes()
		return
	}

	if true {
		key, err := ioutil.ReadFile("fred.txt")
		check(err)
		api := sc.NewAPICombinator(sc.FredEngine{Key: key})
		u, err := url.Parse("https://api.stlouisfed.org/fred/series/observations?series_id=GDP&file_type=xml&observation_start=2000-01-01&observation_end=2020-02-01")
		check(err)
		r, err := api.Get(sc.NewURI(u))
		check(err)
		fmt.Println(show(r))
		return
	}

	if host != "" {
		password := os.Getenv(passwordVar)
		if password == "" {
			panic("needs password")
		}
		ftp, err := sc.NewFTPCombinator(host, user, password)
		check(err)
		r, err := sc.ParseRef("ftp:///" + ftpFile)
		check(err)
		i, err := ftp.Get(r)
		check(err)
		fmt.Println(show(i))
		return
	}

	check(func() error {
		p, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return err
		}
		listRef := sc.NewRef("/logs")
		c, err := sc.NewS3Collection(bucket, "testing", listRef, s3.New(p))
		if err != nil {
			return err
		}
		i, err := c.Get(listRef)
		if err != nil {
			return err
		}
		list, ok := i.([]interface{})
		if !ok {
			return fmt.Errorf("bad type: %T", i)
		}
		var n int
		for _, x := range list {
			fmt.Println(x)
			n++
		}
		fmt.Printf("%d records\n", n)
		return nil
	}())
	return

	check(func() error {

		p, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return err
		}

		storage, err := sc.NewFileSystem("logging/fs")
		if err != nil {
			return err
		}
		listRef := sc.NewRef("/logs")
		collection, err := sc.NewS3Collection(bucket, "dir9", listRef, s3.New(p))
		if err != nil {
			return err
		}
		logger := sc.NewLoggingCombinator(storage, collection, listRef)

		if err := logger.Put(sc.NewRef("a"), "howdy123"); err != nil {
			return err
		}

		{
			i, err := logger.Get(sc.NewRef("a"))
			if err != nil {
				return err
			}
			fmt.Println(show(i))
		}
		return nil

		list := func() error {
			i, err := logger.Get(listRef)
			if err != nil {
				return err
			}
			fmt.Println(show(i))
			return nil
		}

		if err := list(); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			check(logger.Put(sc.NewRef(fmt.Sprintf("item-%d", i)), uuid.New().String()))
		}
		if err := list(); err != nil {
			return err
		}

		return nil
	}())
	return

	check(func() error {
		if bucket == "" {
			return fmt.Errorf("needs bucket")
		}
		p, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return err
		}
		ref := sc.NewRef("/collection")
		c, err := sc.NewS3Collection(bucket, "dir7", ref, s3.New(p))
		if err != nil {
			return err
		}
		payload := map[string]interface{}{
			"test":  123,
			"bogus": []interface{}{"hi!", 4, 5, 6},
		}
		if err := c.Merge(ref, payload); err != nil {
			return err
		}
		i, err := c.Get(ref)
		if err != nil {
			return err
		}
		fmt.Println(show(i))
		return nil
	}())

	return

	if false {
		db, err := OpenDB()
		check(err)
		check(db.Ping())

		c := sc.NewDatabaseCombinator(db)

		info, err := dbInfo()
		check(err)
		var u url.URL
		u.Scheme = "sql"
		u.Path = "/myname"
		v := make(url.Values)
		v.Set("query", info.Query)
		v.Set("format", "csv")
		v.Set("interface", "string")
		u.RawQuery = v.Encode()

		r := sc.NewURI(&u)
		i, err := c.Get(r)
		check(err)
		fmt.Println(show(i))

		return
	}

}

func show(i interface{}) string {
	s, err := sc.Blob(i)
	check(err)
	return string(s)
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
