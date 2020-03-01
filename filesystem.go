package sc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// NewFileSystem creates a new filesystem storage combinator with
// given scheme, mountpoint, and default file mode
// just uses the uri's path to map to underlying file system
// if no path, then hashes the uri string to a path
func NewFileSystem(mount string) (*FileSystem, error) {
	mount = filepath.Clean(mount)
	if mount == "" {
		return nil, fmt.Errorf("needs a mount point")
	}
	if err := mkdir(mount); err != nil {
		return nil, err
	}
	return &FileSystem{mount: mount}, nil
}

// FileSystem is a storage combinator based on files
type FileSystem struct {
	scheme, mount string
}

func mkdir(p string) error {
	if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(p, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func (fs FileSystem) path(r Reference) (string, error) {
	p := r.URI().Path
	if p == "" {
		er, err := encode(r)
		if err != nil {
			return "", err
		}
		p = er.URI().String()
	}
	return filepath.Join(fs.mount, filepath.Clean("/"+p)), nil
}

type FileReference struct {
	Name    string
	Size    int
	IsDir   bool
	ModTime time.Time
}

type Directory []FileReference

func NewFileReference(fi os.FileInfo) FileReference {
	return FileReference{
		Name:    fi.Name(),
		Size:    int(fi.Size()),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}
}

func (f FileReference) URI() *url.URL {
	var u url.URL
	u.Scheme = "file"
	u.Path = path.Clean("/" + f.Name)
	if f.IsDir {
		u.Path += "/"
	}
	return &u
}

func (f FileReference) String() string {
	buf, _ := json.Marshal(f)
	return string(buf)
}

func (fs FileSystem) Get(r Reference) (interface{}, error) {
	p, err := fs.path(r)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return nil, wrapNotFound(r, err)
	}
	if fi.IsDir() {
		list, err := ioutil.ReadDir(p)
		if err != nil {
			return nil, err
		}
		var files Directory
		for _, fi := range list {
			files = append(files, NewFileReference(fi))
		}
		return files, nil
	}
	return ioutil.ReadFile(p)
}

func (fs FileSystem) Put(r Reference, i interface{}) error {
	return fs.put(r, i, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
}

func (fs FileSystem) put(r Reference, i interface{}, flags int) error {
	p, err := fs.path(r)
	if err != nil {
		return err
	}
	if err := mkdir(filepath.Dir(p)); err != nil {
		return err
	}
	write := func(reader io.Reader) error {
		file, err := os.OpenFile(p, flags, 0666)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := io.Copy(file, reader); err != nil {
			return err
		}
		return file.Close()
	}
	var reader io.Reader
	close := func() error {
		return nil
	}
	switch t := i.(type) {
	case []byte:
		reader = bytes.NewReader(t)
	case string:
		reader = strings.NewReader(t)
	case io.Reader:
		reader = t
	case io.ReadCloser:
		reader = t
		close = func() error {
			return t.Close()
		}
	default:
		reader = strings.NewReader(fmt.Sprint(t))
	}
	if err := write(reader); err != nil {
		return err
	}
	return close()
}

func (fs FileSystem) Delete(r Reference) error {
	p, err := fs.path(r)
	if err != nil {
		return err
	}
	return os.RemoveAll(p)
}

// appends to the file or creates it
func (fs FileSystem) Merge(r Reference, i interface{}) error {
	return fs.put(r, i, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
}
