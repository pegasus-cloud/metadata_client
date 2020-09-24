package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/metadata_client/metadata/common"
	"github.com/metadata_client/metadata/utility"

	"github.com/spf13/viper"
)

// HasDeleted ...
func (p *Provider) HasDeleted(messageID string) (deleted bool, err error) {
	url := fmt.Sprintf("%s://%s/_search", p.Schema, p.Endpoint)
	gQuery := gQuery{}
	gMatch := gMatch{}
	gMatch.Match.MessageID = messageID
	gQuery.Query.Bool.Must = append(gQuery.Query.Bool.Must, gMatch)
	bgQuery, err := json.Marshal(gQuery)
	if err != nil {
		return false, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, err)
	}

	// Get metadata from metadata index in Elasticsearch
	metaResp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bgQuery))
	if err != nil {
		return false, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, err)
	} else if status != http.StatusOK {
		return false, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, string(metaResp))
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(metaResp, esQueryResp)

	if len(esQueryResp.Hits.Hits) == 1 && esQueryResp.Hits.Hits[0].Index == viper.GetString("metadata.elasticsearch.msg_deleted_indices") {
		return true, nil
	}
	return false, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", gQuery, common.MessageIDDoesNotExist)
}
