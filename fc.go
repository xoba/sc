package sc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

func NewFileSystem(mount string, mode os.FileMode) (*FileSystem, error) {
	mount = filepath.Clean(mount)
	if err := mkdir(mount, mode); err != nil {
		return nil, err
	}
	return &FileSystem{mount: mount, mode: mode}, nil
}

type FileSystem struct {
	mode  os.FileMode
	mount string
}

type FileReference struct {
	scheme, path string
}

func (r FileReference) Scheme() string {
	return r.scheme
}

func (r FileReference) Path() string {
	return r.path
}

func mkdir(p string, mode os.FileMode) error {
	if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(p, mode); err != nil {
			return err
		}
	}
	return nil
}

func (fs FileSystem) Reference(uri string) (Reference, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
	case "":
		u.Scheme = "file"
	default:
		return nil, fmt.Errorf("illegal scheme: %q", u.Scheme)
	}
	return FileReference{scheme: u.Scheme, path: u.Path}, nil
}

func (fs FileSystem) path(r Reference) string {
	return filepath.Join(fs.mount, r.Path())
}

type File struct {
	Name    string
	Size    int
	IsDir   bool
	ModTime time.Time
}

func (fs FileSystem) Get(r Reference) (interface{}, error) {
	p := fs.path(r)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		list, err := ioutil.ReadDir(p)
		if err != nil {
			return nil, err
		}
		var files []File
		for _, fi := range list {
			files = append(files, File{
				Name:    fi.Name(),
				Size:    int(fi.Size()),
				ModTime: fi.ModTime(),
				IsDir:   fi.IsDir(),
			})
		}
		return files, nil
	}
	return ioutil.ReadFile(p)
}

func (fs FileSystem) Put(r Reference, i interface{}) error {
	p := fs.path(r)
	if err := mkdir(filepath.Dir(p), fs.mode); err != nil {
		return err
	}
	var buf []byte
	switch t := i.(type) {
	case []byte:
		buf = t
	case string:
		buf = []byte(t)
	default:
		buf = []byte(fmt.Sprintf("%v", t))
	}
	return ioutil.WriteFile(p, buf, fs.mode)
}

func (fs FileSystem) Delete(r Reference) error {
	return os.RemoveAll(fs.path(r))
}
