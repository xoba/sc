package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blang/semver"
	_ "github.com/snowflakedb/gosnowflake"
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

func newMultiplexer(m map[string]sc.StorageCombinator) sc.StorageCombinator {
	c, err := sc.NewMultiplexer(m)
	check(err)
	return c
}

func newLister(raw sc.StorageCombinator, listRef sc.Reference) sc.StorageCombinator {
	c, err := sc.NewListingCombinator(raw, listRef)
	check(err)
	return c
}

func NewStorageCombinator(base string, listRef sc.Reference) (sc.StorageCombinator, error) {
	log := func(c sc.StorageCombinator) sc.StorageCombinator {
		return sc.NewPassthrough(fmt.Sprintf("%T", c), c)
	}
	store := log(
		newLister(
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
	a, err := sc.NewAppender(newFilesystem("diskstore"))
	var c sc.StorageCombinator
	c = a
	check(c.Put(sc.NewRef("a.txt"), "hi there A\n"))
	check(c.Put(sc.NewRef("b.txt"), "hi there B\n"))
	check(c.Put(sc.NewRef("c.txt"), "hi there C\n"))
	check(c.Merge(sc.NewRef("a.txt"), "hello again!\n"))
	check(err)
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

type engine struct {
}

func (e engine) Get(r sc.Reference) (*http.Response, error) {
	key, err := ioutil.ReadFile("fred.txt")
	if err != nil {
		return nil, err
	}
	// modify a copy of uri:
	u, err := url.Parse(r.URI().String())
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("api_key", strings.TrimSpace(string(key)))
	u.RawQuery = q.Encode()
	return http.Get(u.String())
}

func main() {

	if retag {
		check(RunRetag())
		return
	}

	if true {
		api, err := sc.NewAPICombinator(engine{})
		check(err)
		u, err := url.Parse("https://api.stlouisfed.org/fred/series/observations?series_id=GDP&file_type=xml&observation_start=1900-01-01&observation_end=2019-12-01")
		check(err)
		r, err := api.Get(sc.NewURI(u))
		check(err)
		fmt.Println(show(r))
		return
	}

	if false {
		db, err := OpenDB()
		check(err)
		check(db.Ping())

		c, err := sc.NewDatabaseCombinator(db)
		check(err)

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
	s, err := sc.Show(i)
	check(err)
	return s
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
