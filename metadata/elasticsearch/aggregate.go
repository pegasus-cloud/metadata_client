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
