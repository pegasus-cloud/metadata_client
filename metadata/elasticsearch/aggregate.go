package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/common"
	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

type (
	esAggregateResp struct {
		Aggregations interface{} `json:"aggregations"`
	}
)

// Aggregate ...
func (p *Provider) Aggregate(rule []byte) (metadata []byte, err error) {
	// According to rule to get metadata from in Elasticsearch
	url := fmt.Sprintf("%s://%s/%s/_search", p.Scheme, p.Endpoint, p.Index)
	metaResp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(rule))
	if err != nil {
		return nil, err
	} else if status != http.StatusOK {
		return nil, errors.New(common.StatusCodeIsNotOK)
	}
	esAggregateResp := &esAggregateResp{}
	json.Unmarshal(metaResp, esAggregateResp)

	aggregate, err := json.Marshal(esAggregateResp.Aggregations)
	if err != nil {
		return nil, err
	}

	return aggregate, nil
}

//Musts ...
type Musts struct {
	Size  int `json:"size"`
	Query struct {
		Bool struct {
			Must []interface{} `json:"must"`
		} `json:"bool"`
	} `json:"query"`
}

//MustsWithAggregate ...
type MustsWithAggregate struct {
	Musts
	Aggs struct {
		Action struct {
			Terms struct {
				Field string `json:"field"`
			} `json:"terms"`
		}
	} `json:"aggs"`
}

//NewMustsWithAggregate ...
func NewMustsWithAggregate(key string) *MustsWithAggregate {
	body := &MustsWithAggregate{}
	body.Aggs.Action.Terms.Field = key
	return body
}

//ToByte ...
func (m *MustsWithAggregate) ToByte() []byte {
	bytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}
	}
	return bytes
}

// QueryString add query_string
func (m *Musts) QueryString(val string) *Musts {
	type esQueryStringBody struct {
		QueryString struct {
			Query string `json:"query"`
		} `json:"query_string"`
	}
	sb := &esQueryStringBody{}
	sb.QueryString.Query = val
	m.append(sb)
	return m
}

//TimestampRange add range for @timestamp with gte and lte
func (m *Musts) TimestampRange(gte, lte string) *Musts {
	type esRangeBody struct {
		Range struct {
			Timestamp struct {
				GTE string `json:"gte"`
				LTE string `json:"lte"`
			} `json:"@timestamp"`
		} `json:"range"`
	}
	rb := &esRangeBody{}
	rb.Range.Timestamp.GTE = gte
	rb.Range.Timestamp.LTE = lte
	m.append(rb)
	return m
}

//TimestampRangeWithTimeZone add range for @timestamp with gte, lte, timezone
// timezone format is +08:00 or +8 or Asia/Taipei or UTC+8
func (m *Musts) TimestampRangeWithTimeZone(timezone, gte, lte string) *Musts {
	type esRangeBody struct {
		Range struct {
			Timestamp struct {
				TimeZone string `json:"time_zone"`
				GTE      string `json:"gte"`
				LTE      string `json:"lte"`
			} `json:"@timestamp"`
		} `json:"range"`
	}
	rb := &esRangeBody{}
	rb.Range.Timestamp.TimeZone = timezone
	rb.Range.Timestamp.GTE = gte
	rb.Range.Timestamp.LTE = lte
	m.append(rb)
	return m
}

func (m *Musts) append(val interface{}) {
	m.Query.Bool.Must = append(m.Query.Bool.Must, val)
}

//ToByte ...
func (m *Musts) ToByte() []byte {
	bytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}
	}
	return bytes
}
