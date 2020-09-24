package elasticsearch

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/utility"
)

type setup struct {
	Setting struct {
		Shards   int `json:"index.number_of_shards"`
		Replicas int `json:"index.number_of_replicas"`
	} `json:"settings"`
}

// Setup ..
func (p *Provider) Setup() (err error) {
	indices := []string{p.DeletedIndex, p.Index}
	for _, index := range indices {
		url := fmt.Sprintf("%s://%s/%s", p.Schema, p.Endpoint, index)
		setup := setup{}
		setup.Setting.Replicas = p.NumOfReplicas
		setup.Setting.Shards = p.NumOfShards

		bsetup, err := json.Marshal(setup)
		if err != nil {
			return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Setup", setup, err)
		}
		if resp, status, err := utility.SendRequest(http.MethodPut, url, headers, bsetup); err != nil {
			return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Setup", setup, err)
		} else if status != http.StatusOK {
			return fmt.Errorf("[%s](%+v) %v", "Elasticsearch Setup", setup, string(resp))
		}
	}
	return nil
}
