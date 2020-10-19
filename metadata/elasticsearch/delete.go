package elasticsearch

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/common"
	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

// Delete ...
func (p *Provider) Delete(messageID string) (err error) {
	// Get Message from Elasticsearch
	message, err := p.Get(messageID)
	if err != nil {
		return err
	}

	// Insert into DeletedIndex
	if err := p.insert2DeletedIndex(messageID, message); err != nil {
		return err
	}

	// Delete message in Elasticsearch
	url := fmt.Sprintf("%s://%s/%s/_doc/%s", p.Scheme, p.Endpoint, p.Index, messageID)
	_, status, err := utility.SendRequest(http.MethodDelete, url, nil, nil)
	if err != nil {
		return err
	} else if status != http.StatusOK {
		return errors.New(common.StatusCodeIsNotOK)
	}
	return nil
}
