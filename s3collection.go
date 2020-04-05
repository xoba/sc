package sc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
)

// maintains a collection of json-encoded records
type S3Collection struct {
	bucket string
	prefix string
	svc    *s3.S3
	ref    Reference
}

type S3Record struct {
	ID        string // unique ID of this record, for instance to de-dup
	Timestamp time.Time
	Payload   interface{}
}

const MaxKeys = 10

// ref is the one single valid reference for Get and Merge methods
func NewS3Collection(bucket, prefix string, ref Reference, svc *s3.S3) (*S3Collection, error) {
	if bucket == "" {
		return nil, fmt.Errorf("needs bucket")
	}
	if strings.HasPrefix(prefix, "/") {
		return nil, fmt.Errorf("prefix can't start with '/'")
	}
	u := ref.URI()
	if u.RawQuery != "" {
		return nil, fmt.Errorf("can't have query")
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("can't have fragment")
	}
	if u.User != nil {
		return nil, fmt.Errorf("can't have user")
	}
	return &S3Collection{
		bucket: bucket,
		prefix: prefix,
		svc:    svc,
		ref:    ref,
	}, nil
}

func (c S3Collection) refMatches(r Reference) bool {
	norm := func(r Reference) string {
		u := *(r.URI())
		u.Fragment = ""
		u.RawQuery = ""
		u.User = nil
		return u.String()
	}
	return norm(c.ref) == norm(r)
}

// think about query "after=isotime" or before="isotime" for only those ones,
// or fragment "count" for just the count
func (c S3Collection) Get(r Reference) (interface{}, error) {
	if !c.refMatches(r) {
		return nil, NotFound
	}
	load := func(key string) ([]S3Record, error) {
		resp, err := c.svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, wrapNotFound(r, err)
		}
		defer resp.Body.Close()
		d := json.NewDecoder(resp.Body)
		var out []S3Record
		for {
			var x S3Record
			if err := d.Decode(&x); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			out = append(out, x)
		}
		return out, nil
	}
	var keys []string
	records := make(map[string]S3Record)
	var marker string
	for {
		resp, err := c.svc.ListObjects(&s3.ListObjectsInput{
			Bucket:  aws.String(c.bucket),
			Marker:  aws.String(marker),
			MaxKeys: aws.Int64(1000),
			Prefix:  aws.String(c.prefix),
		})
		if err != nil {
			return nil, err
		}
		for _, o := range resp.Contents {
			keys = append(keys, *o.Key)
			recs, err := load(*o.Key)
			if err != nil {
				return nil, err
			}
			for _, x := range recs {
				records[x.ID] = x
			}
		}
		if *resp.IsTruncated {
			marker = *resp.Contents[len(resp.Contents)-1].Key
		} else {
			break
		}
	}
	var sorted []S3Record
	for _, x := range records {
		sorted = append(sorted, x)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})
	fmt.Printf("%d keys\n", len(keys))
	if len(keys) > MaxKeys {
		if err := c.consolidate(keys, sorted); err != nil {
			return nil, err
		}
	}
	var out []interface{}
	for _, x := range sorted {
		out = append(out, x.Payload)
	}
	return out, nil
}

func serialize(recs ...S3Record) ([]byte, error) {
	w := new(bytes.Buffer)
	gz := gzip.NewWriter(w)
	e := json.NewEncoder(gz)
	e.SetEscapeHTML(false)
	for _, r := range recs {
		if err := e.Encode(r); err != nil {
			return nil, err
		}
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (c S3Collection) store(recs ...S3Record) error {
	if len(recs) == 0 {
		return fmt.Errorf("nothing to store")
	}
	buf, err := serialize(recs...)
	if err != nil {
		return err
	}
	if _, err := c.svc.PutObject(&s3.PutObjectInput{
		Bucket:          aws.String(c.bucket),
		Key:             aws.String(path.Join(c.prefix, uuid.New().String()) + ".json.gz"),
		Body:            bytes.NewReader(buf),
		ContentType:     aws.String("application/json"),
		ContentEncoding: aws.String("gzip"),
	}); err != nil {
		return err
	}
	return nil
}

func (c S3Collection) consolidate(keys []string, records []S3Record) error {
	fmt.Printf("consolidating %d keys\n", len(keys))
	if err := c.store(records...); err != nil {
		return err
	}
	delete := &s3.Delete{
		Quiet: aws.Bool(true),
	}
	doi := &s3.DeleteObjectsInput{
		Bucket: aws.String(c.bucket),
		Delete: delete,
	}
	for _, k := range keys {
		delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{
			Key: aws.String(k),
		})
	}
	if _, err := c.svc.DeleteObjects(doi); err != nil {
		return err
	}
	return nil
}

func (c S3Collection) Merge(r Reference, i interface{}) error {
	if !c.refMatches(r) {
		return NotFound
	}
	s3r := S3Record{
		ID:        uuid.New().String(),
		Timestamp: time.Now().UTC(),
		Payload:   i,
	}
	if err := c.store(s3r); err != nil {
		return err
	}
	return nil
}

func (c S3Collection) Put(r Reference, i interface{}) error {
	return unimplemented(c, "Put")
}

func (c S3Collection) Delete(r Reference) error {
	return unimplemented(c, "Delete")
}
