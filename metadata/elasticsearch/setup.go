package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/pegasus-cloud/metadata_client/metadata/common"
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
		url := fmt.Sprintf("%s://%s/%s", p.Scheme, p.Endpoint, index)
		setup := setup{}
		setup.Setting.Replicas = p.NumOfReplicas
		setup.Setting.Shards = p.NumOfShards

		bsetup, err := json.Marshal(setup)
		if err != nil {
			return err
		}
		if _, status, err := utility.SendRequest(http.MethodPut, url, headers, bsetup); err != nil {
			return err
		} else if status != http.StatusOK {
			return errors.New(common.StatusCodeIsNotOK)
		}
	}
	return nil
}
