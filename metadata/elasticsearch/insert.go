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
	eMetadata struct {
		Index struct {
			Index string `json:"_index"`
			Type  string `json:"_type"`
			ID    string `json:"_id"`
		} `json:"index"`
	}
)

// Insert ...
func (p *Provider) Insert(id string, metadata []byte) (err error) {
	return p.insert(p.Index, id, metadata)
}

func (p *Provider) insert2DeletedIndex(id string, metadata []byte) (err error) {
	return p.insert(p.DeletedIndex, id, metadata)
}

func (p *Provider) insert(index, id string, metadata []byte) (err error) {
	var body []byte
	url := fmt.Sprintf("%s://%s/_bulk?refresh=%t", p.Scheme, p.Endpoint, p.Refresh)

	metaStruct := &eMetadata{}
	metaStruct.Index.Index = index
	metaStruct.Index.Type = "_doc"
	metaStruct.Index.ID = id
	ebMetadata, err := json.Marshal(metaStruct)
	if err != nil {
		return err
	}

	body = append(body, ebMetadata...)
	body = append(body, '\n')
	body = append(body, metadata...)
	body = append(body, '\n')

	// Send data to Elasticsearch
	if _, status, err := utility.SendRequest(http.MethodPut, url, headers, bytes.NewBuffer(body)); err != nil {
		return err
	} else if status != http.StatusOK {
		return errors.New(common.StatusCodeIsNotOK)
	}
	return nil
}
