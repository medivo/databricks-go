package databricks

// PrincipalName is a container type for a name that is either a user name or a
// group name.
type PrincipalName struct {
	UserName  *string `json:"user_name,omitempty"`
	GroupName *string `json:"group_name,omitempty"`
}
