package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/common"
	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

// Insert ...
func (p *Provider) Insert(message common.Metadata) (err error) {
	url := fmt.Sprintf("%s://%s/%s/_doc?refresh=%t", p.Scheme, p.Endpoint, p.Index, p.Refresh)
	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	}

	// Send data to Elasticsearch
	if resp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bMessage)); err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	} else if status != http.StatusCreated {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), string(resp))
	}
	return nil
}

func (p *Provider) insert2DeletedIndex(message common.Metadata) (err error) {
	url := fmt.Sprintf("%s://%s/%s/_doc?refresh=%t", p.Scheme, p.Endpoint, p.DeletedIndex, p.Refresh)
	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	}

	// Send data to Elasticsearch
	if resp, status, err := utility.SendRequest(http.MethodPost, url, headers, bytes.NewBuffer(bMessage)); err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), err)
	} else if status != http.StatusCreated {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Insert", string(bMessage), string(resp))
	}
	return nil
}
