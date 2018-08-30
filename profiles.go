package databricks

// InstanceProfile is an IAM instance profile that can be attached to instances
// when launching a cluster.
type InstanceProfile struct {
	InstanceProfileARN string `json:"instance_profile_arn"`
}
