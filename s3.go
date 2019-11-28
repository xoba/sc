package sc

import (
	"bytes"
	"encoding/json"
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

func (o S3Reference) URI() *url.URL {
	var u url.URL
	u.Scheme = "s3"
	u.Host = o.Bucket
	u.Path = o.Key
	return &u
}

func (o S3Reference) String() string {
	buf, _ := json.Marshal(o)
	return string(buf)
}

func NewS3KeyValue(scheme, bucket, prefix string) (*S3KeyValue, error) {
	if scheme == "" {
		scheme = "s3"
	}
	if bucket == "" {
		return nil, fmt.Errorf("needs bucket")
	}
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
			continue
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
		return fs.parseS3URI(r.URI())
	}
}

func (fs S3KeyValue) parseS3URI(u *url.URL) (*S3Reference, error) {
	var s3ref S3Reference
	if strings.ToLower(u.Scheme) == "s3" && u.Host != "" {
		s3ref.Bucket = u.Host
	} else {
		s3ref.Bucket = fs.bucket
	}
	s3ref.Key = removeLeadingSlashes(u.Path)
	return &s3ref, nil

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
	case io.ReadSeeker:
		rs = t
	default:
		return fmt.Errorf("don't know how to handle object type %T", t)
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

func (fs S3KeyValue) Merge(r Reference, i interface{}) error {
	return unimplemented(fs, "Merge")
}

func (fs S3KeyValue) Delete(r Reference) error {
	s3ref, err := fs.s3ref(r)
	if err != nil {
		return err
	}
	if _, err := fs.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s3ref.Bucket),
		Key:    aws.String(s3ref.Key),
	}); err != nil {
		return err
	}
	return nil
}

func (fs S3KeyValue) Find(q string) (Reference, error) {
	u, err := url.Parse(q)
	if err != nil {
		return nil, err
	}
	s3ref, err := fs.parseS3URI(u)
	if err != nil {
		return nil, err
	}
	if _, err := fs.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s3ref.Bucket),
		Key:    aws.String(s3ref.Key),
	}); err != nil {
		return nil, err
	}
	return s3ref, nil
}
