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

// HasDeleted ...
func (p *Provider) HasDeleted(messageID string) (deleted bool, err error) {
	// Defined Elasticseaerch query body
	url := fmt.Sprintf("%s://%s/_search", p.Scheme, p.Endpoint)
	gQuery := gQuery{}
	gQuery.Query.Match.ID = messageID
	bgQuery, err := json.Marshal(gQuery)
	if err != nil {
		return false, err
	}

	// Get metadata from metadata index in Elasticsearch
	metaResp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bgQuery))
	if err != nil {
		return false, err
	} else if status != http.StatusOK {
		return false, errors.New(common.StatusCodeIsNotOK)
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(metaResp, esQueryResp)

	// If reponse length is one and it exist in DeletedIndex, than return true
	if len(esQueryResp.Hits.Hits) == 1 && esQueryResp.Hits.Hits[0].Index == p.DeletedIndex {
		return true, nil
	}
	return false, errors.New(common.MessageIDDoesNotExist)
}
