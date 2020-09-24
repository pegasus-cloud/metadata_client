package metadata

import (
	"github.com/metadata_client/metadata/common"
	"github.com/metadata_client/metadata/elasticsearch"
	"github.com/spf13/viper"
)

// Metadata ...
type Metadata interface {
	Insert(message common.Metadata) (err error)
	Get(messageID string) (metadata common.Metadata, err error)
	Delete(messageID string) (err error)
	Purge(groupID, queueName string, force bool) (err error)
	HasDeleted(messageID string) (deleted bool, err error)
	Setup() (err error)
	HealthCheck() (status string, err error)
}

const (
	// ElasticsearchProviderType declares that metadata provider is elasticsearch
	ElasticsearchProviderType = "elasticsearch"
)

// MetadataProvider ...
var MetadataProvider Metadata

// Init ...
func Init(providerName string) {
	MetadataProvider = setConfig(providerName)
}

// New ...
func New(providerName string) (MetadataProvider Metadata) {
	return setConfig(providerName)
}

func setConfig(providerName string) (metadataProvider Metadata) {
	var provider Metadata
	switch providerName {
	case ElasticsearchProviderType:
		esProvider := &elasticsearch.Provider{
			Schema:       elasticsearch.Schema,
			Endpoint:     elasticsearch.Endpoint,
			Index:        elasticsearch.Index,
			DeletedIndex: elasticsearch.DeletedIndex,
		}
		if viper.GetString("metadata.elasticsearch.schema") == "" {
			esProvider.Schema = viper.GetString("metadata.elasticsearch.schema")
		}
		if viper.GetString("metadata.elasticsearch.endpoint") == "" {
			esProvider.Endpoint = viper.GetString("metadata.elasticsearch.endpoint")
		}
		if viper.GetString("metadata.elasticsearch.attribute_indices") == "" {
			esProvider.Index = viper.GetString("metadata.elasticsearch.attribute_indices")
		}
		if viper.GetString("metadata.elasticsearch.msg_deleted_indices") == "" {
			esProvider.DeletedIndex = viper.GetString("metadata.elasticsearch.msg_deleted_indices")
		}
		provider = esProvider
	default:
		panic(common.ProviderNameDoesNotSupport)
	}
	return provider
}

// Use ...
func Use() (Metadata Metadata) {
	return MetadataProvider
}
