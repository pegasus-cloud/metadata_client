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
func (p *Provider) Get(messageID string) (metadata interface{}, err error) {
	url := fmt.Sprintf("%s://%s/_search", p.Scheme, p.Endpoint)
	gQuery := gQuery{}
	gQuery.Query.Match.ID = messageID

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
		return metadata, fmt.Errorf(common.MessageIDHasBeenDeleted)
	}

	metadata = esQueryResp.Hits.Hits[0].Source
	return metadata, nil
}
