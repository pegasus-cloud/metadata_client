package elasticsearch

var (
	headers = map[string]string{
		"Content-Type": "application/json",
	}
)

// Provider ...
type Provider struct {
	Schema        string
	Endpoint      string
	Index         string
	DeletedIndex  string
	NumOfReplicas int
	NumOfShards   int
	_             struct{}
}
