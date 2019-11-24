package sc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func NewFileSystem(scheme, mount string, mode os.FileMode) (*FileSystem, error) {
	mount = filepath.Clean(mount)
	if err := mkdir(mount, mode); err != nil {
		return nil, err
	}
	return &FileSystem{scheme: scheme, mount: mount, mode: mode}, nil
}

type FileSystem struct {
	mode          os.FileMode
	scheme, mount string
}

func mkdir(p string, mode os.FileMode) error {
	if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(p, mode); err != nil {
			return err
		}
	}
	return nil
}

func (fs FileSystem) Reference(p string) (*Reference, error) {
	return &Reference{Scheme: fs.scheme, Path: p}, nil
}

func (fs FileSystem) path(r *Reference) string {
	return filepath.Join(fs.mount, filepath.Clean("/"+r.Path))
}

type File struct {
	Name    string
	Size    int
	IsDir   bool
	ModTime time.Time
}

func (fs FileSystem) goodRef(r *Reference) error {
	if r.Scheme != fs.scheme {
		return fmt.Errorf("bad scheme: %q", r.Scheme)
	}
	return nil
}

func (fs FileSystem) Get(r *Reference) (interface{}, error) {
	if err := fs.goodRef(r); err != nil {
		return nil, err
	}
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

func (fs FileSystem) Put(r *Reference, i interface{}) error {
	if err := fs.goodRef(r); err != nil {
		return err
	}
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

func (fs FileSystem) Delete(r *Reference) error {
	if err := fs.goodRef(r); err != nil {
		return err
	}
	return os.RemoveAll(fs.path(r))
}
