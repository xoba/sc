package sc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type FTPCombinator struct {
	host, user, password string
}

func NewFTPCombinator(host, user, password string) (*FTPCombinator, error) {
	ftp := &FTPCombinator{
		host:     host,
		user:     user,
		password: password,
	}
	return ftp, nil
}

func (f FTPCombinator) login() (*sftp.Client, error) {
	config := &ssh.ClientConfig{
		User: f.user,
		Auth: []ssh.AuthMethod{
			ssh.Password(f.password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", f.host), config)
	if err != nil {
		return nil, err
	}
	return sftp.NewClient(conn)
}

func (f FTPCombinator) Get(r Reference) (interface{}, error) {
	s, err := f.login()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	u := r.URI()
	p := u.Path
	for {
		if !strings.HasPrefix(p, "/") {
			break
		}
		p = p[1:]
	}
	type listing struct {
		Size int64
		Name string
	}
	list := func(dir string) (interface{}, error) {
		w := new(bytes.Buffer)
		e := json.NewEncoder(w)
		e.SetEscapeHTML(false)
		list, err := s.ReadDir(p)
		if err != nil {
			return nil, err
		}
		for _, fi := range list {
			if err := e.Encode(listing{Size: fi.Size(), Name: fi.Name()}); err != nil {
				return nil, err
			}
		}
		return w.Bytes(), nil
	}
	if len(p) == 0 {
		return list(".")
	}
	file, err := s.Open(p)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return list(p)
	}
	w := new(bytes.Buffer)
	n, err := io.Copy(w, file)
	if err != nil {
		return nil, err
	}
	if n != fi.Size() {
		return nil, fmt.Errorf("expected %d bytes, got %d", fi.Size(), n)
	}
	return w.Bytes(), nil
}

func (f FTPCombinator) Put(Reference, interface{}) error {
	return unimplemented(f, "Put")
}
func (f FTPCombinator) Delete(Reference) error {
	return unimplemented(f, "Delete")
}
func (f FTPCombinator) Merge(Reference, interface{}) error {
	return unimplemented(f, "Merge")
}
