package sc

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
)

const (
	Algo      = "AES_256"
	KeyLength = 32
)

// uses aws kms master key data encryption to encrypt/decrypt
// in relation to embedded combinator
type Encrypter struct {
	svc   *kms.KMS
	keyID string
	c     StorageCombinator
}

func NewEncrypter(svc *kms.KMS, keyID string, c StorageCombinator) (*Encrypter, error) {
	e := Encrypter{
		svc:   svc,
		keyID: keyID,
		c:     c,
	}
	const test = "hello world"
	enc, err := e.encrypt([]byte(test))
	if err != nil {
		return nil, err
	}
	dec, err := e.decrypt(enc)
	if err != nil {
		return nil, err
	}
	if string(dec) != test {
		return nil, fmt.Errorf("kms test failed")
	}
	return &e, nil
}

func (e Encrypter) Get(r Reference) (interface{}, error) {
	i, err := e.c.Get(r)
	if err != nil {
		return nil, err
	}
	enc, err := resolve(i)
	if err != nil {
		return nil, err
	}
	dec, err := e.decrypt(enc)
	if err != nil {
		return nil, err
	}
	return dec, nil
}

func (e Encrypter) Put(r Reference, i interface{}) error {
	return e.update(r, i, e.c.Put)
}

func (e Encrypter) Delete(r Reference) error {
	return e.c.Delete(r)
}

func (e Encrypter) Merge(r Reference, i interface{}) error {
	return e.update(r, i, e.c.Merge)
}

func (e Encrypter) update(r Reference, i interface{}, f func(Reference, interface{}) error) error {
	buf, err := resolve(i)
	if err != nil {
		return err
	}
	enc, err := e.encrypt(buf)
	if err != nil {
		return err
	}
	return f(r, enc)
}

func resolve(i interface{}) ([]byte, error) {
	cp := func(r io.Reader) ([]byte, error) {
		w := new(bytes.Buffer)
		if _, err := io.Copy(w, r); err != nil {
			return nil, err
		}
		return w.Bytes(), nil
	}
	switch t := i.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case io.Reader:
		return cp(t)
	case io.ReadCloser:
		x, err := cp(t)
		if err != nil {
			return nil, err
		}
		if err := t.Close(); err != nil {
			return nil, err
		}
		return x, nil
	default:
		return nil, fmt.Errorf("unhandled type: %T", t)
	}
}

func (e Encrypter) encrypt(data []byte) ([]byte, error) {
	ko, err := e.svc.GenerateDataKey(&kms.GenerateDataKeyInput{
		KeyId:   aws.String(e.keyID),
		KeySpec: aws.String(Algo),
	})
	if err != nil {
		return nil, err
	}
	if n := len(ko.Plaintext); n != KeyLength {
		return nil, fmt.Errorf("got %d bytes, expected %d", n, KeyLength)
	}
	enc, err := Encrypt(data, ko.Plaintext)
	if err != nil {
		return nil, err
	}
	w := new(bytes.Buffer)
	n := int64(len(ko.CiphertextBlob))
	if err := binary.Write(w, binary.BigEndian, n); err != nil {
		return nil, err
	}
	w.Write(ko.CiphertextBlob)
	w.Write(enc)
	return w.Bytes(), nil
}

func (e Encrypter) decrypt(in []byte) ([]byte, error) {
	r := bytes.NewReader(in)
	var n int64
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	key := make([]byte, n)
	x, err := r.Read(key)
	if err != nil {
		return nil, err
	}
	if x != int(n) {
		return nil, fmt.Errorf("key size mismatch: %d vs %d", x, n)
	}
	o, err := e.svc.Decrypt(&kms.DecryptInput{
		CiphertextBlob: key,
	})
	if err != nil {
		return nil, err
	}
	if n := len(o.Plaintext); n != KeyLength {
		return nil, fmt.Errorf("got %d bytes, expected %d", n, KeyLength)
	}
	w := new(bytes.Buffer)
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	return Decrypt(w.Bytes(), o.Plaintext)
}

func Encrypt(data, key []byte) ([]byte, error) {
	block, _ := aes.NewCipher(key)
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

func Decrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
