package sc

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type DatabaseCombinator struct {
	db *sql.DB
}

func NewDatabaseCombinator(db *sql.DB) (*DatabaseCombinator, error) {
	return &DatabaseCombinator{db: db}, nil
}

func (dc DatabaseCombinator) Get(r Reference) (interface{}, error) {

	q := r.URI().Query()

	queryProc := func(key string, required bool, defaultValue string) (string, error) {
		v := strings.TrimSpace(q.Get(key))
		if required && len(v) == 0 {
			return "", fmt.Errorf("needs a %q parameter", key)
		}
		if v == "" && defaultValue != "" {
			v = defaultValue
		}
		return v, nil
	}

	query, err := queryProc("query", true, "")
	if err != nil {
		return nil, err
	}
	format, err := queryProc("format", true, "")
	if err != nil {
		return nil, err
	}
	na, err := queryProc("na", false, "NA")
	if err != nil {
		return nil, err
	}
	outputType, err := queryProc("interface", false, "bytes")
	if err != nil {
		return nil, err
	}

	rows, err := dc.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var row []interface{}
	for _, c := range cols {
		var f interface{}
		switch t := c.ScanType().Name(); t {
		case "float64":
			f = &sql.NullFloat64{}
		case "int64":
			f = &sql.NullInt64{}
		case "string":
			f = &sql.NullString{}
		case "bool":
			f = &sql.NullBool{}
		case "Time":
			f = &sql.NullTime{}
		default:
			return nil, fmt.Errorf("unhandled database type %q", t)
		}
		row = append(row, f)
	}
	w := new(bytes.Buffer)
	flush := func() error {
		return nil
	}
	var writer func(map[string]interface{}) error
	{
		header := make(map[string]interface{})
		switch format {
		case "csv":
			e := csv.NewWriter(w)
			flush = func() error {
				e.Flush()
				return nil
			}
			writer = func(m map[string]interface{}) error {
				var row []string
				for _, c := range cols {
					v, ok := m[c.Name()]
					if ok {
						var x string
						switch t := v.(type) {
						case float64:
							x = strconv.FormatFloat(t, 'g', -1, 64)
						case string:
							x = t
						case int64:
							x = strconv.FormatInt(t, 10)
						case bool:
							x = strconv.FormatBool(t)
						case time.Time:
							x = t.UTC().Format("2006-01-02T15:04:05.000Z")
						default:
							return fmt.Errorf("unhandled format type: %T", t)
						}
						row = append(row, x)
					} else {
						row = append(row, na)
					}
				}
				return e.Write(row)
			}
			for _, c := range cols {
				header[c.Name()] = c.Name()
			}
		case "json":
			e := json.NewEncoder(w)
			e.SetEscapeHTML(false)
			writer = func(m map[string]interface{}) error {
				return e.Encode(m)
			}
			for _, c := range cols {
				header[c.Name()] = c.ScanType().Name()
			}
		default:
			return nil, fmt.Errorf("unhandled format: %q", format)
		}
		if err := writer(header); err != nil {
			return nil, err
		}
	}
	for rows.Next() {
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, x := range row {
			var v interface{}
			switch t := x.(type) {
			case *sql.NullFloat64:
				if t.Valid {
					v = t.Float64
				}
			case *sql.NullInt64:
				if t.Valid {
					v = t.Int64
				}
			case *sql.NullBool:
				if t.Valid {
					v = t.Bool
				}
			case *sql.NullString:
				if t.Valid {
					v = t.String
				}
			case *sql.NullTime:
				if t.Valid {
					v = t.Time
				}
			default:
				return nil, fmt.Errorf("unhandled scan type: %T", t)
			}
			if v != nil {
				m[cols[i].Name()] = v
			}
		}
		if err := writer(m); err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	switch outputType {
	case "string":
		return w.String(), nil
	case "reader":
		return w, nil
	default:
		return w.Bytes(), nil
	}
}

func (dc DatabaseCombinator) Put(Reference, interface{}) error {
	panic("")
}

func (dc DatabaseCombinator) Delete(Reference) error {
	return fmt.Errorf("can't delete from sql combinator; %w", NotSupported)
}

func (dc DatabaseCombinator) Merge(Reference, interface{}) error {
	return fmt.Errorf("can't delete from sql combinator; %w", NotSupported)
}
