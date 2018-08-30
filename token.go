package databricks

// PublicTokenInfo is a data structure that describes the public metadata of an
// access token.
type PublicTokenInfo struct {
	TokenID      string `json:"token_id"`
	CreationTime int64  `json:"creation_time"`
	ExpiryTime   int64  `json:"expiry_time"`
	Comment      string `json:"comment"`
}
