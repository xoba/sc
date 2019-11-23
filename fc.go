package sc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"
)

func NewFileCombinator(mount string, mode os.FileMode) (*FileSystem, error) {
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
	u *url.URL
}

func (r FileReference) String() string {
	return r.u.String()
}

func (r FileReference) URI() url.URL {
	return *r.u
}

func mkdir(path string, mode os.FileMode) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, mode); err != nil {
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
	return FileReference{u: u}, nil
}

func (fs FileSystem) path(r Reference) string {
	return filepath.Join(fs.mount, path.Clean(r.URI().Path))
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
	path := fs.path(r)
	if err := mkdir(filepath.Dir(path), fs.mode); err != nil {
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
	return ioutil.WriteFile(path, buf, fs.mode)
}

func (fs FileSystem) Delete(r Reference) error {
	return os.RemoveAll(fs.path(r))
}
