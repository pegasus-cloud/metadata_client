package common

type (
	// Metadata ...
	Metadata struct {
		VersionOfStruct        string             `json:"versionOfStruct"`
		MessageID              string             `json:"messageId"`
		UserID                 string             `json:"userId"`
		GroupID                string             `json:"groupId"`
		QueueName              string             `json:"queueName"`
		IsEncrypted            bool               `json:"isEncrypted"`
		KMSID                  string             `json:"kmsId"`
		SendTimestamp          string             `json:"sendTimestamp"`
		DisplayName            string             `json:"displayName"`
		MessageAttributes      []MessageAttribute `json:"messageAttributes"`
		MD5OfMessageAttributes string             `json:"md5OfMessageAttributes"`
		MD5OfMessageBody       string             `json:"md5OfMessageBody"`
	}
	// MessageAttribute ...
	MessageAttribute struct {
		Name  string `json:"name"`
		Value struct {
			DataType    string `json:"dataType"`
			StringValue string `json:"stringValue"`
		} `json:"value"`
	}
)

const (
	// HealthOK ...
	HealthOK string = "Health_OK"
	// HealthWarn ...
	HealthWarn string = "Health_Warn"
	// HealthError ...
	HealthError string = "Health_Err"
	// HealthUnknown ...
	HealthUnknown string = "Unknown"
)

// Str2Map ...
func (m *Metadata) Str2Map() (mm map[string]interface{}) {
	mm = map[string]interface{}{}
	mm["VersionOfStruct"] = m.VersionOfStruct
	mm["MessageID"] = m.MessageID
	mm["UserID"] = m.UserID
	mm["GroupID"] = m.GroupID
	mm["QueueName"] = m.QueueName
	mm["IsEncrypted"] = m.IsEncrypted
	mm["KMSID"] = m.KMSID
	mm["SendTimeStamp"] = m.SendTimestamp
	mm["MD5OfMessageAttributes"] = m.MD5OfMessageAttributes
	mm["MD5OfMessageBody"] = m.MD5OfMessageBody
	return
}
