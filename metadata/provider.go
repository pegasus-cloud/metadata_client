package metadata

// Metadata ...
type Metadata interface {
	Insert(messageID string, message interface{}) (err error)
	Get(messageID string) (metadata interface{}, err error)
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
