package metadata

import (
	"github.com/pegasus-cloud/metadata_client/metadata/common"
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

var metadataProvider Metadata

// type MD interface{}

// Init ...
func Init(provider Metadata) {
	metadataProvider = provider
}

// Use ...
func Use() (Metadata Metadata) {
	return metadataProvider
}
