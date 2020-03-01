package sc

import (
	"time"

	"github.com/google/uuid"
)

type LoggingCombinator struct {
	storage StorageCombinator
	list    StorageCombinator
	listRef Reference
}

// logs get/put/merge/delete calls
func NewLoggingCombinator(storage, list StorageCombinator, listRef Reference) *LoggingCombinator {
	return &LoggingCombinator{
		storage: storage,
		list:    list,
		listRef: listRef,
	}
}

type LogRecord struct {
	ID        string
	SourceURI string
	TargetURI string
	Method    string
	Timestamp time.Time
}

func newLogRecord(method string, source Reference) (*LogRecord, Reference, error) {
	target, err := encode(source)
	if err != nil {
		return nil, nil, err
	}
	record := &LogRecord{
		ID:        uuid.New().String(),
		SourceURI: source.URI().String(),
		TargetURI: target.URI().String(),
		Method:    method,
		Timestamp: time.Now().UTC(),
	}
	return record, target, nil
}

func (c LoggingCombinator) log(record *LogRecord) error {
	if err := c.list.Merge(c.listRef, record); err != nil {
		return err
	}
	return nil
}

func (c LoggingCombinator) Get(r Reference) (interface{}, error) {
	if r.URI().String() == c.listRef.URI().String() {
		return c.list.Get(c.listRef)
	}
	record, target, err := newLogRecord("get", r)
	if err != nil {
		return nil, err
	}
	i, err := c.storage.Get(target)
	if err != nil {
		return nil, err
	}
	if err := c.log(record); err != nil {
		return nil, err
	}
	return i, err
}

type mutatorFunc func(Reference, interface{}) error

func (c LoggingCombinator) update(r Reference, i interface{}, method string, mutator mutatorFunc) error {
	record, target, err := newLogRecord(method, r)
	if err != nil {
		return err
	}
	if err := mutator(target, i); err != nil {
		return err
	}
	return c.log(record)
}

func (c LoggingCombinator) Put(r Reference, i interface{}) error {
	return c.update(r, i, "put", c.storage.Put)
}

func (c LoggingCombinator) Merge(r Reference, i interface{}) error {
	return c.update(r, i, "merge", c.storage.Merge)
}

func (c LoggingCombinator) Delete(r Reference) error {
	return c.update(r, nil, "delete", func(r Reference, i interface{}) error {
		return c.storage.Delete(r)
	})
}
