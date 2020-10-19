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
	esQueryResp struct {
		Hits struct {
			Hits []struct {
				Index  string      `json:"_index"`
				ID     string      `json:"_id"`
				Source interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	gQuery struct {
		Query struct {
			Match struct {
				ID string `json:"_id"`
			} `json:"match"`
		} `json:"query"`
	}
)

// Get ...
func (p *Provider) Get(messageID string) (metadata []byte, err error) {
	// Defined Elasticseaerch query body
	gQuery := gQuery{}
	gQuery.Query.Match.ID = messageID
	bgQuery, err := json.Marshal(gQuery)
	if err != nil {
		return nil, err
	}

	// Get metadata from metadata index in Elasticsearch
	url := fmt.Sprintf("%s://%s/_search", p.Scheme, p.Endpoint)
	metaResp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bgQuery))
	if err != nil {
		return nil, err
	} else if status != http.StatusOK {
		return nil, errors.New(common.StatusCodeIsNotOK)
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(metaResp, esQueryResp)

	// If response length from Elasticsearch is 0, then return MessageIDDoesNotExist error
	// If index of specified document is DeletedIndex, then return MessageIDHasBeenDeleted error
	if len(esQueryResp.Hits.Hits) == 0 {
		return nil, errors.New(common.MessageIDDoesNotExist)
	} else if esQueryResp.Hits.Hits[0].Index == p.DeletedIndex {
		return nil, errors.New(common.MessageIDHasBeenDeleted)
	}

	// Parse Elasticsearch reponse to specified structure
	bSource, err := json.Marshal(esQueryResp.Hits.Hits[0].Source)
	if err != nil {
		return nil, err
	}

	metadata = bSource
	return metadata, nil
}
