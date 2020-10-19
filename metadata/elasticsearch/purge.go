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
	pQuery struct {
		Query struct {
			Bool struct {
				Must []pMatch `json:"must"`
			} `json:"bool"`
		} `json:"query"`
	}
	pMatch struct {
		Match map[string]string `json:"match"`
	}
)

// Purge ...
func (p *Provider) Purge(groupID, queueName string, force bool) (err error) {
	if !force {
		// Get Message from Elasticsearch
		gMetadatas, err := p.get(groupID, queueName)
		if err != nil {
			return err
		}

		// Insert into DeletedIndex
		for _, gMetadata := range gMetadatas {
			if err := p.insert2DeletedIndex(gMetadata.MessageID, gMetadata.MetaData); err != nil {
				return err
			}
		}
	}

	pQuery := pQuery{}
	pQuery.Query.Bool.Must = append(pQuery.Query.Bool.Must, pMatch{
		Match: map[string]string{"groupId": groupID},
	}, pMatch{
		Match: map[string]string{"queueName": queueName},
	})
	bpQuery, err := json.Marshal(pQuery)
	if err != nil {
		return err
	}

	// Purge metadatas in Elasticsearch
	url := fmt.Sprintf("%s://%s/%s/_delete_by_query?refresh=%t", p.Scheme, p.Endpoint, p.Index, p.Refresh)
	if _, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bpQuery)); err != nil {
		return err
	} else if status != http.StatusOK {
		return errors.New(common.StatusCodeIsNotOK)
	}
	return nil
}

type gMetadata struct {
	MessageID string
	MetaData  []byte
}

func (p *Provider) get(groupID, queueName string) (metadatas []gMetadata, err error) {
	pQuery := pQuery{}
	pQuery.Query.Bool.Must = append(pQuery.Query.Bool.Must, pMatch{
		Match: map[string]string{"groupId": groupID},
	}, pMatch{
		Match: map[string]string{"queueName": queueName},
	})
	bpQuery, err := json.Marshal(pQuery)
	if err != nil {
		return nil, fmt.Errorf("[%s](%+v) %v", "JSON Marshal", pQuery, err)
	}

	// Get metadata from Elasticsearch
	url := fmt.Sprintf("%s://%s/_search", p.Scheme, p.Endpoint)
	resp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bpQuery))
	if err != nil {
		return nil, err
	} else if status != http.StatusOK {
		return nil, errors.New(common.StatusCodeIsNotOK)
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(resp, esQueryResp)

	for _, metadata := range esQueryResp.Hits.Hits {
		bSource, err := json.Marshal(metadata.Source)
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, gMetadata{
			MessageID: metadata.ID,
			MetaData:  bSource,
		})
	}
	return metadatas, nil
}
