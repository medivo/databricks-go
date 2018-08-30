package databricks

// ACLItem is an item representing an ACL rule applied to the given principal
// (user or group) on the associated scope point.
type ACLItem struct {
	Principal  string `json:"principal"`
	Permission string `json:"permission"` // TODO(daniel): should this be a type?
}

// SecretMetadata is the metadata about a secret. Returned when listing
// secrets. Does not contain the actual secret value.
type SecretMetadata struct {
	Key                  string `json:"key"`
	LastUpdatedTimestamp int64  `json:"last_updated_timestamp"`
}

// SecretScope is an organizational resource for storing secrets. Secret scopes
// can be different types, and ACLs can be applied to control permissions for
// all secrets within a scope.
type SecretScope struct {
	Name        string
	BackendType string // TODO(daniel)
}
