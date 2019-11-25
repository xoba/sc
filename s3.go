package sc

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3KeyValue struct {
	scheme, bucket, prefix string
	svc                    *s3.S3
}

type S3Reference struct {
	Bucket, Key string
	Public      bool
}

func (o S3Reference) URI() url.URL {
	var u url.URL
	u.Scheme = "s3"
	u.Host = o.Bucket
	u.Path = o.Key
	return u
}

func NewS3KeyValue(scheme, bucket, prefix string) (*S3KeyValue, error) {
	p, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	return &S3KeyValue{
		scheme: scheme,
		bucket: bucket,
		prefix: prefix,
		svc:    s3.New(p),
	}, nil
}

func removeLeadingSlashes(p string) string {
	for {
		if strings.HasPrefix(p, "/") {
			p = p[1:]
		}
		break
	}
	return p
}

func (fs S3KeyValue) s3ref(r Reference) (*S3Reference, error) {
	switch t := r.(type) {
	case S3Reference:
		return &t, nil
	case *S3Reference:
		return t, nil
	default:
		u := r.URI()
		var s3ref S3Reference
		if strings.ToLower(u.Scheme) == "s3" && u.Host != "" {
			s3ref.Bucket = u.Host
		} else {
			s3ref.Bucket = fs.bucket
		}
		s3ref.Key = removeLeadingSlashes(u.Path)
		return &s3ref, nil
	}
}

func (fs S3KeyValue) Get(r Reference) (interface{}, error) {
	s3ref, err := fs.s3ref(r)
	if err != nil {
		return nil, err
	}
	resp, err := fs.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3ref.Bucket),
		Key:    aws.String(s3ref.Key),
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
	case fmt.Stringer:
		rs = strings.NewReader(t.String())
	default:
		w := new(bytes.Buffer)
		fmt.Fprintf(w, "%v", t)
		rs = bytes.NewReader(w.Bytes())
	}
	s3ref, err := fs.s3ref(r)
	if err != nil {
		return err
	}
	poi := s3.PutObjectInput{
		Bucket: aws.String(s3ref.Bucket),
		Key:    aws.String(s3ref.Key),
		Body:   rs,
	}
	if s3ref.Public {
		poi.ACL = aws.String("public-read")
	}
	if _, err := fs.svc.PutObject(&poi); err != nil {
		fmt.Printf("oops: %v\n", err)
		return err
	}
	return nil
}

func (fs S3KeyValue) Delete(r Reference) error {
	return fmt.Errorf("Delete unimplemented")
}
