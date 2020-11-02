package metadata

import (
	"github.com/pegasus-cloud/metadata_client/metadata/elasticsearch"
)

// Metadata ...
type Metadata interface {
	Insert(messageID string, metadata []byte) (err error)
	Get(messageID string) (metadata []byte, err error)
	Delete(messageID string) (err error)
	Purge(groupID, queueName string, force bool) (err error)
	Exist(messageID string) (exist bool, err error)
	Setup() (err error)
	HealthCheck() (status string, err error)
	Aggregate(rule []byte) (metadata []byte, err error)
}

type ProviderEnum int

const (
	//NonProviderEnum ...
	NonProviderEnum ProviderEnum = iota
	//ElasticsearchEnum ...
	ElasticsearchEnum
)

var (
	metadataProvider Metadata
	//ProviderName ...
	ProviderName = NonProviderEnum
)

// Init ...
func Init(provider Metadata) {
	switch provider.(type) {
	case *elasticsearch.Provider:
		ProviderName = ElasticsearchEnum
	}
	metadataProvider = provider
}

// Use ...
func Use() (Metadata Metadata) {
	return metadataProvider
}
