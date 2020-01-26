package sc

import (
	"fmt"

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
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", host), config)
	if err != nil {
		return nil, err
	}
	sftp, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}
	defer sftp.Close()
	w := sftp.Walk(".")
	for w.Step() {
		if w.Err() != nil {
			continue
		}
		fmt.Println(w.Path())
	}
	return ftp, nil
}

func (f FTPCombinator) Get(Reference) (interface{}, error) {
	return nil, unimplemented(f, "Get")
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
