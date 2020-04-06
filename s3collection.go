package sc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	debug  bool
}

type S3Record struct {
	ID        string // unique ID of this record, for instance to de-dup
	Timestamp time.Time
	Payload   interface{}
}

const MaxKeys = 10

func NewS3Collection(bucket, prefix string, ref Reference, svc *s3.S3) (*S3Collection, error) {
	return NewS3CollectionDebug(bucket, prefix, ref, svc, false)
}

// ref is the one single valid reference for Get and Merge methods
func NewS3CollectionDebug(bucket, prefix string, ref Reference, svc *s3.S3, debug bool) (*S3Collection, error) {
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
		debug:  debug,
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

	{
		var n int
		var listError error
		if err := c.svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:  aws.String(c.bucket),
			MaxKeys: aws.Int64(1000),
			Prefix:  aws.String(c.prefix),
		}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
			if c.debug {
				fmt.Fprintf(os.Stderr, "going to list/load %d/%d records\n", n, len(output.Contents))
			}
			for _, o := range output.Contents {
				n++
				keys = append(keys, *o.Key)
				recs, err := load(*o.Key)
				if err != nil {
					listError = err
					return false
				}
				for _, x := range recs {
					records[x.ID] = x
				}
			}
			return true
		}); err != nil {
			return nil, err
		}

		if listError != nil {
			return nil, listError
		}
	}

	var sorted []S3Record
	for _, x := range records {
		sorted = append(sorted, x)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})
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

// divides a list into sub-lists of maximal length
func divide(list []string, max int) (out [][]string) {
	if len(list) == 0 {
		return nil
	} else if len(list) < max {
		return [][]string{list}
	}
	left, right := halve(list)
	out = append(out, divide(left, max)...)
	out = append(out, divide(right, max)...)
	return
}

// divides a list roughly in two
func halve(list []string) (left, right []string) {
	for _, x := range list {
		if len(left) < len(right) {
			left = append(left, x)
		} else {
			right = append(right, x)
		}
	}
	return
}

func DeleteKeys(svc *s3.S3, bucket string, keys []string) error {
	for _, list := range divide(keys, 1000) {
		doi := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{
				Quiet: aws.Bool(true),
			},
		}
		for _, k := range list {
			doi.Delete.Objects = append(doi.Delete.Objects, &s3.ObjectIdentifier{
				Key: aws.String(k),
			})
		}
		if _, err := svc.DeleteObjects(doi); err != nil {
			return err
		}
	}
	return nil
}

func (c S3Collection) delete(keys []string) error {
	return DeleteKeys(c.svc, c.bucket, keys)
}

func (c S3Collection) consolidate(keys []string, records []S3Record) error {
	if c.debug {
		fmt.Fprintf(os.Stderr, "consolidating %d keys\n", len(keys))
	}
	if err := c.store(records...); err != nil {
		return err
	}
	if err := c.delete(keys); err != nil {
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
