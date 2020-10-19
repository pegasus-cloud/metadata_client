package elasticsearch

import (
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

// Delete ...
func (p *Provider) Delete(messageID string) (err error) {
	// Get Message from Elasticsearch
	message, err := p.Get(messageID)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", messageID, err)
	}

	// Insert into DeletedIndex
	if err := p.insert2DeletedIndex(messageID, message); err != nil {
		return err
	}

	url := fmt.Sprintf("%s://%s/%s/_doc/%s", p.Scheme, p.Endpoint, p.Index, messageID)

	// Delete message in Elasticsearch
	resp, status, err := utility.SendRequest(http.MethodDelete, url, nil, nil)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", messageID, err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", messageID, string(resp))
	}
	return nil
}
