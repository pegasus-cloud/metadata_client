package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/common"
	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

type (
	gQuery struct {
		Query struct {
			Bool struct {
				Must []gMatch `json:"must"`
			} `json:"bool"`
		} `json:"query"`
	}
	gMatch struct {
		Match struct {
			MessageID string `json:"messageId"`
		} `json:"match"`
	}
	esQueryResp struct {
		Hits struct {
			Hits []struct {
				Index  string          `json:"_index"`
				Source common.Metadata `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
)

// Get ...
func (p *Provider) Get(messageID string) (metadata common.Metadata, err error) {
	url := fmt.Sprintf("%s://%s/_search", p.Scheme, p.Endpoint)
	metadata = common.Metadata{}
	gQuery := gQuery{}
	gMatch := gMatch{}
	gMatch.Match.MessageID = messageID
	gQuery.Query.Bool.Must = append(gQuery.Query.Bool.Must, gMatch)
	bgQuery, err := json.Marshal(gQuery)
	if err != nil {
		return metadata, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, err)
	}

	// Get metadata from metadata index in Elasticsearch
	metaResp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bgQuery))
	if err != nil {
		return metadata, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, err)
	} else if status != http.StatusOK {
		return metadata, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, string(metaResp))
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(metaResp, esQueryResp)

	if len(esQueryResp.Hits.Hits) == 0 {
		return metadata, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, common.MessageIDDoesNotExist)
	} else if esQueryResp.Hits.Hits[0].Index == p.DeletedIndex {
		if err := p.deleteInDeletedIndex(messageID); err != nil {
			return metadata, err
		}
		return metadata, fmt.Errorf(common.MessageIDHasBeenDeleted)
	}

	metadata = esQueryResp.Hits.Hits[0].Source
	return metadata, nil
}
