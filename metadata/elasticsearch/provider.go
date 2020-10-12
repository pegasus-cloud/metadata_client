package elasticsearch

var (
	headers = map[string]string{
		"Content-Type": "application/json",
	}
)

// Provider ...
type Provider struct {
	Scheme        string
	Endpoint      string
	Index         string
	DeletedIndex  string
	NumOfReplicas int
	NumOfShards   int
	Refresh       bool
	_             struct{}
}
