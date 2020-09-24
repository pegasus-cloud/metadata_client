package elasticsearch

import (
	"fmt"
	"net/http"

	"github.com/metadata_client/metadata/utility"
)

type (
	dQuery struct {
		Query struct {
			Bool struct {
				Must []dMatch `json:"must"`
			} `json:"bool"`
		} `json:"query"`
	}
	dMatch struct {
		Match struct {
			MessageID string `json:"messageId"`
		} `json:"match"`
	}
)

// Delete ...
func (p *Provider) Delete(messageID string) (err error) {
	// Get Message from Elasticsearch
	message, err := p.Get(messageID)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Get", messageID, err)
	}

	// Insert into DeletedIndex
	if err := p.insert2DeletedIndex(message); err != nil {
		return err
	}

	dQuery := dQuery{}
	dMatch := dMatch{}
	dMatch.Match.MessageID = messageID
	dQuery.Query.Bool.Must = append(dQuery.Query.Bool.Must, dMatch)
	url := fmt.Sprintf("%s://%s/%s/_delete_by_query?refresh=true", p.Schema, p.Endpoint, p.Index)

	// Delete message in Elasticsearch
	resp, status, err := utility.SendRequest(http.MethodPost, url, headers, dQuery)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", dQuery, err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", dQuery, string(resp))
	}
	return nil
}

func (p *Provider) deleteInDeletedIndex(messageID string) (err error) {
	dQuery := dQuery{}
	dMatch := dMatch{}
	dMatch.Match.MessageID = messageID
	dQuery.Query.Bool.Must = append(dQuery.Query.Bool.Must, dMatch)
	url := fmt.Sprintf("%s://%s/%s/_delete_by_query?refresh=true", p.Schema, p.Endpoint, p.DeletedIndex)

	// Delete message in Elasticsearch
	resp, status, err := utility.SendRequest(http.MethodPost, url, headers, dQuery)
	if err != nil {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", dQuery, err)
	} else if status != http.StatusOK {
		return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Delete", dQuery, string(resp))
	}
	return nil
}
