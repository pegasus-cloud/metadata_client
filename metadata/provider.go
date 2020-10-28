package metadata

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

var metadataProvider Metadata

// Init ...
func Init(provider Metadata) {
	metadataProvider = provider
}

// Use ...
func Use() (Metadata Metadata) {
	return metadataProvider
}
