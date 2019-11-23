package sc

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3KeyValue struct {
	bucket, prefix string
	svc            *s3.S3
}

func NewS3KeyValue(bucket, prefix string) (*S3KeyValue, error) {
	p, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	return &S3KeyValue{
		bucket: bucket,
		prefix: prefix,
		svc:    s3.New(p),
	}, nil
}

type S3Reference struct {
	u *url.URL
}

func (s S3Reference) URI() url.URL {
	return *s.u
}
func (s S3Reference) String() string {
	return s.u.String()
}

func (fs S3KeyValue) Reference(p string) (Reference, error) {
	u, err := url.Parse(p)
	if err != nil {
		return nil, err
	}
	return S3Reference{u: u}, nil
}

func (fs S3KeyValue) key(r Reference) string {
	p := path.Join(fs.prefix, path.Clean("/"+r.URI().Path))
	for {
		if strings.HasPrefix(p, "/") {
			p = p[1:]
			continue
		}
		break
	}
	return p
}

func (fs S3KeyValue) Get(r Reference) (interface{}, error) {
	resp, err := fs.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.key(r)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	w := new(bytes.Buffer)
	if _, err := io.Copy(w, resp.Body); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (fs S3KeyValue) Put(r Reference, i interface{}) error {
	var rs io.ReadSeeker
	switch t := i.(type) {
	case string:
		rs = strings.NewReader(t)
	case []byte:
		rs = bytes.NewReader(t)
	default:
		w := new(bytes.Buffer)
		fmt.Fprintf(w, "%v", t)
		rs = bytes.NewReader(w.Bytes())
	}
	_, err := fs.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.key(r)),
		Body:   rs,
	})
	if err != nil {
		return err
	}
	return nil
}

func (fs S3KeyValue) Delete(Reference) error {
	return fmt.Errorf("Delete unimplemented")
}
