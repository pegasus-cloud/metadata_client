package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

type (
	eMetadata struct {
		Index struct {
			Index string `json:"_index"`
			Type  string `json:"_type"`
			ID    string `json:"_id"`
		} `json:"index"`
	}
)

// Insert ...
func (p *Provider) Insert(id string, metadata interface{}) (err error) {
	var body []byte
	url := fmt.Sprintf("%s://%s/_bulk?refresh=%t", p.Scheme, p.Endpoint, p.Refresh)

	metaStruct := &eMetadata{}
	metaStruct.Index.Index = p.Index
	metaStruct.Index.Type = "_doc"
	metaStruct.Index.ID = id
	ebMetadata, err := json.Marshal(metaStruct)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(ebMetadata), err)
	}

	bMetadata, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMetadata), err)
	}

	body = append(body, ebMetadata...)
	body = append(body, '\n')
	body = append(body, bMetadata...)
	body = append(body, '\n')

	// Send data to Elasticsearch
	if resp, status, err := utility.SendRequest(http.MethodPut, url, headers, bytes.NewBuffer(body)); err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMetadata), err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMetadata), string(resp))
	}
	return nil
}

func (p *Provider) insert2DeletedIndex(id string, message interface{}) (err error) {
	var body []byte
	url := fmt.Sprintf("%s://%s/%s/_bulk?refresh=%t", p.Scheme, p.Endpoint, p.DeletedIndex, p.Refresh)

	metaStruct := &eMetadata{}
	metaStruct.Index.Index = p.DeletedIndex
	metaStruct.Index.Type = "_doc"
	metaStruct.Index.ID = id
	bMetadata, err := json.Marshal(metaStruct)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMetadata), err)
	}

	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	}

	body = append(body, bMetadata...)
	body = append(body, '\n')
	body = append(body, bMessage...)
	body = append(body, '\n')

	// Send data to Elasticsearch
	if resp, status, err := utility.SendRequest(http.MethodPut, url, headers, bytes.NewBuffer(body)); err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), string(resp))
	}
	return nil
}
