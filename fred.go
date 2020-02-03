package sc

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type FredEngine struct {
	Key []byte
}

func (e FredEngine) Get(r Reference) (*http.Response, error) {
	// modify a copy of uri:
	u, err := url.Parse(r.URI().String())
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("api_key", strings.TrimSpace(string(e.Key)))
	u.RawQuery = q.Encode()
	return http.Get(u.String())
}

type Observations struct {
	Start        string         `xml:"observation_start,attr"`
	End          string         `xml:"observation_end,attr"`
	Observations []*Observation `xml:"observation"`
}

type Observation struct {
	Date  string  `xml:"date,attr"`
	Value float64 `xml:"value,attr"`
}

func (o Observations) String() string {
	buf, _ := json.MarshalIndent(o, "", "  ")
	return string(buf)
}

func (e FredEngine) Process(rc io.ReadCloser) (interface{}, error) {
	defer rc.Close()
	d := xml.NewDecoder(rc)
	var f Observations
	if err := d.Decode(&f); err != nil {
		return nil, err
	}
	w := new(bytes.Buffer)
	w2 := csv.NewWriter(w)
	if err := w2.Write([]string{"date", "value"}); err != nil {
		return nil, err
	}
	for _, o := range f.Observations {
		if err := w2.Write([]string{o.Date, fmt.Sprintf("%f", o.Value)}); err != nil {
			return nil, err
		}
	}
	w2.Flush()
	if err := w2.Error(); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
