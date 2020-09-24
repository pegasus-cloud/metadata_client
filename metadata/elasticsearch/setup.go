package elasticsearch

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/metadata_client/metadata/utility"

	"github.com/spf13/viper"
)

type setup struct {
	Setting struct {
		Shards   int `json:"index.number_of_shards"`
		Replicas int `json:"index.number_of_replicas"`
	} `json:"settings"`
}

const (
	replicas = 3
	shards   = 3
)

// Setup ..
func (p *Provider) Setup() (err error) {
	indices := []string{viper.GetString("metadata.elasticsearch.msg_deleted_indices"), viper.GetString("metadata.elasticsearch.attribute_indices")}
	for _, index := range indices {
		url := fmt.Sprintf("%s://%s/%s", p.Schema, p.Endpoint, index)
		setup := setup{}

		if viper.GetInt("metadata.elasticsearch.number_of_replicas") != 0 {
			setup.Setting.Replicas = viper.GetInt("metadata.elasticsearch.number_of_replicas")
		} else {
			setup.Setting.Replicas = replicas
		}

		if viper.GetInt("metadata.elasticsearch.number_of_shards") != 0 {
			setup.Setting.Shards = viper.GetInt("metadata.elasticsearch.number_of_shards")
		} else {
			setup.Setting.Shards = shards
		}

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
