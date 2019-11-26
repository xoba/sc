package sc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"
)

// NewFileSystem creates a new filesystem storage combinator with
// given scheme, mountpoint, and default file mode
func NewFileSystem(scheme, mount string, mode os.FileMode) (*FileSystem, error) {
	mount = filepath.Clean(mount)
	if err := mkdir(mount, mode); err != nil {
		return nil, err
	}
	return &FileSystem{scheme: scheme, mount: mount, mode: mode}, nil
}

// FileSystem is a storage combinator based on files
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

func (fs FileSystem) path(r Reference) string {
	return fs.getPath(r.URI().Path)
}

func (fs FileSystem) getPath(p string) string {
	return filepath.Join(fs.mount, filepath.Clean("/"+p))
}

type FileReference struct {
	Name    string
	Size    int
	IsDir   bool
	ModTime time.Time
}

func NewFileReference(fi os.FileInfo) FileReference {
	return FileReference{
		Name:    fi.Name(),
		Size:    int(fi.Size()),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}
}

func (f FileReference) URI() url.URL {
	var u url.URL
	u.Scheme = "file"
	u.Path = path.Clean("/" + f.Name)
	if f.IsDir {
		u.Path += "/"
	}
	return u
}

func (f FileReference) String() string {
	buf, _ := json.Marshal(f)
	return string(buf)
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
		var files []FileReference
		for _, fi := range list {
			files = append(files, NewFileReference(fi))
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

func (fs FileSystem) Find(q string) (Reference, error) {
	p := fs.getPath(q)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	r := NewFileReference(fi)
	r.Name = path.Clean("/" + q)
	return r, nil
}

func (fs FileSystem) Merge(r Reference, i interface{}) error {
	return unimplemented(fs, "Merge")
}
