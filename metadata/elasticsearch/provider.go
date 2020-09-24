package elasticsearch

var (
	headers = map[string]string{
		"Content-Type": "application/json",
	}
)

const (
	// Schema ..
	Schema string = "http"
	// Endpoint ...
	Endpoint string = "127.0.0.1:9200"
	// Index ...
	Index string = "metadata_index"
	// DeletedIndex ...
	DeletedIndex string = "deleted_index"
)

// Provider ...
type Provider struct {
	Schema       string
	Endpoint     string
	Index        string
	DeletedIndex string
	_            struct{}
}
