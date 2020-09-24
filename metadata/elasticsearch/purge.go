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
		metadatas, err := p.get(groupID, queueName)
		if err != nil {
			return err
		}

		// Insert into DeletedIndex
		for _, metadata := range metadatas {
			if err := p.insert2DeletedIndex(metadata); err != nil {
				return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert2DeletedIndex", metadata, err)
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
		return fmt.Errorf("[%s](%+v) %v", "JSON Marshal", pQuery, err)
	}

	// Purge metadatas in Elasticsearch
	url := fmt.Sprintf("%s://%s/%s/_delete_by_query?refresh=true", p.Schema, p.Endpoint, p.Index)
	if resp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bpQuery)); err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Purge", pQuery, err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Purge", pQuery, string(resp))
	}
	return nil
}

func (p *Provider) get(groupID, queueName string) (metadatas []common.Metadata, err error) {
	metadatas = []common.Metadata{}
	pQuery := pQuery{}
	pQuery.Query.Bool.Must = append(pQuery.Query.Bool.Must, pMatch{
		Match: map[string]string{"groupId": groupID},
	}, pMatch{
		Match: map[string]string{"queueName": queueName},
	})
	bpQuery, err := json.Marshal(pQuery)
	if err != nil {
		return metadatas, fmt.Errorf("[%s](%+v) %v", "JSON Marshal", pQuery, err)
	}

	// Get metadata from Elasticsearch
	url := fmt.Sprintf("%s://%s/_search", p.Schema, p.Endpoint)
	resp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bpQuery))
	if err != nil {
		return metadatas, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", bpQuery, err)
	} else if status != http.StatusOK {
		return metadatas, fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", bpQuery, string(resp))
	}
	esQueryResp := &esQueryResp{}
	json.Unmarshal(resp, esQueryResp)

	for _, metadata := range esQueryResp.Hits.Hits {
		metadatas = append(metadatas, common.Metadata{
			VersionOfStruct:        metadata.Source.VersionOfStruct,
			MessageID:              metadata.Source.MessageID,
			UserID:                 metadata.Source.UserID,
			GroupID:                metadata.Source.GroupID,
			QueueName:              metadata.Source.QueueName,
			IsEncrypted:            metadata.Source.IsEncrypted,
			KMSID:                  metadata.Source.KMSID,
			SendTimestamp:          metadata.Source.SendTimestamp,
			DisplayName:            metadata.Source.DisplayName,
			MessageAttributes:      metadata.Source.MessageAttributes,
			MD5OfMessageAttributes: metadata.Source.MD5OfMessageAttributes,
			MD5OfMessageBody:       metadata.Source.MD5OfMessageBody,
		})
	}
	return metadatas, nil
}
