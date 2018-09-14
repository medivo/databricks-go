package databricks

// ClusterCreateRequest is a Create request for a Cluster.
type ClusterCreateRequest struct {
	NumWorkers             *int32            `json:"num_workers,omitempty"`
	Autoscale              *Autoscale        `json:"autoscale,omitempty"`
	ClusterID              string            `json:"cluster_id"`
	ClusterName            string            `json:"cluster_name"`
	SparkVersion           string            `json:"spark_version"`
	SparkConf              *S3StorageInfo    `json:"spark_conf,omitempty"`
	AWSAttributes          *AWSAttributes    `json:"aws_attributes,omitempty"`
	NodeTypeID             string            `json:"node_type_id"`
	DriverNodeTypeID       string            `json:"driver_node_type_id"`
	SSHPublicKeys          []string          `json:"ssh_public_keys"`
	CustomTags             []ClusterTag      `json:"custom_tags"`
	ClusterLogConf         *ClusterLogConf   `json:"cluster_log_conf,omitempty"`
	InitScripts            []InitScriptInfo  `json:"init_scripts"`
	SparkEnvVars           map[string]string `json:"spark_env_vars,omitempty"`
	AutoterminationMinutes int32             `json:"autotermination_minutes"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk"`
}

// ClusterEditRequest is a Edit request for a Cluster.
type ClusterEditRequest struct {
	NumWorkers             *int32            `json:"num_workers,omitempty"`
	Autoscale              *Autoscale        `json:"autoscale,omitempty"`
	ClusterID              string            `json:"cluster_id"`
	ClusterName            string            `json:"cluster_name"`
	SparkVersion           string            `json:"spark_version"`
	SparkConf              *S3StorageInfo    `json:"spark_conf,omitempty"`
	AWSAttributes          *AWSAttributes    `json:"aws_attributes,omitempty"`
	NodeTypeID             string            `json:"node_type_id"`
	DriverNodeTypeID       string            `json:"driver_node_type_id"`
	SSHPublicKeys          []string          `json:"ssh_public_keys,omitempty"`
	CustomTags             []ClusterTag      `json:"custom_tags,omitempty"`
	ClusterLogConf         ClusterLogConf    `json:"cluster_log_conf"`
	InitScripts            []InitScriptInfo  `json:"init_scripts,omitempty"`
	SparkEnvVars           map[string]string `json:"spark_env_vars,omitempty"`
	AutoterminationMinutes int32             `json:"autotermination_minutes"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk"`
}

// ClusterGetResponse is a response for a Cluster Get request.
type ClusterGetResponse struct {
	NumWorkers             *int32            `json:"num_workers,omitempty"`
	Autoscale              *Autoscale        `json:"autoscale,omitempty"`
	ClusterID              string            `json:"cluster_id"`
	CreatorUserName        string            `json:"creator_user_name"`
	Driver                 *SparkNode        `json:"driver"`
	Executors              []SparkNode       `json:"executors"`
	SparkContextID         int64             `json:"spark_context_id"`
	JDBCPort               int32             `json:"jdbc_port"`
	ClusterName            string            `json:"cluster_name"`
	SparkVersion           string            `json:"spark_version"`
	SparkConf              map[string]string `json:"spark_conf"`
	AWSAttributes          AWSAttributes     `json:"aws_attributes"`
	NodeTypeID             string            `json:"node_type_id"`
	DriverNodeTypeID       string            `json:"driver_node_type_id"`
	SSHPublicKeys          []string          `json:"ssh_public_keys"`
	CustomTags             map[string]string `json:"custom_tags"`
	ClusterLogConf         *ClusterLogConf   `json:"cluster_log_conf"`
	InitScripts            []InitScriptInfo  `json:"init_scripts"`
	SparkEnvVars           map[string]string `json:"spark_env_vars"`
	AutoterminationMinutes int32             `json:"autotermination_minutes"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk"`
	ClusterSource          *AWSAvailability  `json:"cluster_source"`
	State                  ClusterState      `json:"state"`
	StateMessage           string            `json:"state_message"`
	StartTime              int64             `json:"start_time"`
	TerminatedTime         int64             `json:"terminated_time"`
	LastStateLossTime      int64             `json:"last_state_loss_time"`
	LastActivityTime       int64             `json:"last_activity_time"`
	ClusterMemoryMB        int64             `json:"cluster_memory_mb"`
	ClusterCores           float32           `json:"cluster_cores"`
	DefaultTags            map[string]string `json:"default_tags"`
}

// Autoscale is used to set the bounds on autoscaling a Cluster.
type Autoscale struct {
	Min int32 `json:"min_workers"`
	Max int32 `json:"max_workers"`
}

// ParameterPair is a termination parameter.
type ParameterPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// AWSAvailability is used to configure AWS availability.
type AWSAvailability string

const (
	Spot             AWSAvailability = "SPOT"
	OnDemand                         = "ON_DEMAND"
	SpotWithFallBack                 = "SPOT_WITH_FALLBACK"
)

// EBSVolumeType EBS volume types that Databricks supports. See Amazon EBS
// Product Details for details.
type EBSVolumeType string

const (
	SSD EBSVolumeType = "GENERAL_PURPOSE_SSD"
	HDD               = "THROUGHPUT_OPTIMIZED_HDD"
)

// ClusterSource is the service that created the cluster.
type ClusterSource string

const (
	UI         ClusterSource = "UI"
	ClusterJob               = "JOB"
	API                      = "API"
)

// ClusterState is the state of a cluster. The current allowable state
// transitions are as follows:
//
// PENDING -> RUNNING
// PENDING -> TERMINATING
// RUNNING -> RESIZING
// RUNNING -> RESTARTING
// RUNNING -> TERMINATING
// RESTARTING -> RUNNING
// RESTARTING -> TERMINATING
// RESIZING -> RUNNING
// RESIZING -> TERMINATING
// TERMINATING -> TERMINATED
type ClusterState string

const (
	Pending     ClusterState = "PENDING"
	Running                  = "RUNNING"
	Terminating              = "TERMINATING"
	Resizing                 = "RESIZING"
	Restarting               = "RESTARTING"
	Terminated               = "TERMINATED"
)

// TerminationReason is the reason why a Cluster terminated.
type TerminationReason struct {
	Code       EBSVolumeType     `json:"code"`
	Parameters map[string]string `json:"parameters"`
}

// S3StorageInfo is S3 storage information.
type S3StorageInfo struct {
	Destination      string `json:"destination"`
	Region           string `json:"region"`
	Endpoint         string `json:"endpoint"`
	EnableEncryption bool   `json:"enable_encryption"`
	EncryptionType   string `json:"encryption_type"`
	KMSKey           string `json:"kms_key"`
	CannedACL        string `json:"canned_acl"`
}

// DbfsStorageInfo is DBFS storage info.
type DbfsStorageInfo struct {
	Destination string `json:"destination"`
}

// SparkVersion represents a Databricks Spark version.
type SparkVersion struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

// SparkConfPair are Spark configuration key-value pairs.
type SparkConfPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SparkEnvPair are Spark environment variable key-value pairs.
type SparkEnvPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SparkNodeAwsAttributes are attributes specific to AWS for a Spark node.
type SparkNodeAwsAttributes struct {
	IsSpot bool `json:"is_spot"`
}

// SparkNode is a Spark Node.
type SparkNode struct {
	PrivateIP         string                 `json:"private_ip"`
	PublicDNS         string                 `json:"public_dns"`
	NodeID            string                 `json:"node_id"`
	InstanceID        string                 `json:"instance_id"`
	StartTimestamp    int64                  `json:"start_timestamp"`
	NodeAWSAttributes SparkNodeAwsAttributes `json:"node_aws_attributes"`
	HostPrivateIP     string                 `json:"host_private_ip"`
}

// NodeType is a AWS node type.
type NodeType struct {
	NodeTypeID     string  `json:"node_type_id"`
	MemoryMB       int32   `json:"memory_mb"`
	NumCores       float32 `json:"num_cores"`
	Description    string  `json:"description"`
	InstanceTypeID string  `json:"instance_type_id"`
	IsDeprecated   bool    `json:"is_deprecated"`
}

// AWSAttributes is used to set AWS attributes.
type AWSAttributes struct {
	FirstOnDemand       int32           `json:"first_on_demand"`
	Availability        AWSAvailability `json:"availability"`
	ZoneID              string          `json:"zone_id"`
	InstanceProfileARN  *string         `json:"instance_profile_arn,omitempty"`
	SpotBidPricePercent *int32          `json:"spot_bid_price_percent,omitempty"`
	EBSVolumeType       *EBSVolumeType  `json:"ebs_volume_type,omitempty"`
	EBSVolumeCount      *int32          `json:"ebs_volume_count,omitempty"`
	EBSVolumeSize       *int32          `json:"ebs_volume_size,omitempty"`
}

// ClusterTag is a key value pair of cluster tags.
type ClusterTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ClusterLogConf is used to configure Cluster logging.
type ClusterLogConf struct {
	DBFS *DbfsStorageInfo `json:"dbfs"`
	S3   *S3StorageInfo   `json:"s3"`
}

// InitScriptInfo is info for an init script.
type InitScriptInfo struct {
	DBFS *DbfsStorageInfo `json:"dbfs"`
	S3   *S3StorageInfo   `json:"s3"`
}

// LogSyncStatus is the statu of log synchronization
type LogSyncStatus struct {
	LastAttempted int64  `json:"last_attempted"`
	LastException string `json:"last_exception"`
}

// ClusterInfo describes all of the metadata about a single Spark cluster in
// Databricks.
type ClusterInfo struct {
	NumWorkers             *int32            `json:"num_workers"`
	Autoscale              *Autoscale        `json:"autoscale"`
	ClusterID              string            `json:"cluster_id"`
	CreatorUserName        string            `json:"creator_user_name"`
	Driver                 SparkNode         `json:"driver"`
	Executors              []SparkNode       `json:"executors"`
	SparkContextID         int64             `json:"spark_context_id"`
	JDBCPort               int32             `json:"jdbc_port"`
	ClusterName            string            `json:"cluster_name"`
	SparkVersion           string            `json:"spark_version"`
	SparkConf              S3StorageInfo     `json:"spark_conf"` //Example Spark confs: {"spark.speculation": true, "spark.streaming.ui.retainedBatches": 5} or {"spark.driver.extraJavaOptions": "-verbose:gc -XX:+PrintGCDetails"}
	AWSAttributes          AWSAttributes     `json:"aws_attributes"`
	NodeTypeID             string            `json:"node_type_id"`
	DriverNodeTypeID       string            `json:"driver_node_type_id"`
	SSHPublicKeys          []string          `json:"ssh_public_keys"`
	CustomTags             []ClusterTag      `json:"custom_tags"`
	ClusterLogConf         ClusterLogConf    `json:"cluster_log_conf"`
	InitScripts            []InitScriptInfo  `json:"init_scripts"`
	SparkEnvVars           SparkEnvPair      `json:"spark_env_vars"`
	AutoterminationMinutes int32             `json:"autotermination_minutes"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk"`
	ClusterSource          AWSAvailability   `json:"cluster_source"`
	State                  ClusterState      `json:"state"`
	StateMessage           string            `json:"state_message"`
	Start                  int64             `json:"start"`
	TerminatedTime         int64             `json:"terminated_time"`
	LastStateLossTime      int64             `json:"last_state_loss_time"`
	LastActivityTime       int64             `json:"last_activity_time"`
	ClusterMemoryMB        int64             `json:"cluster_memory_mb"`
	ClusterCores           float32           `json:"cluster_cores"`
	DefaultTags            map[string]string `json:"default_tags"`
	ClusterLogStatus       LogSyncStatus     `json:"cluster_log_status"`
	TerminationReason      TerminationReason `json:"termination_reason"`
}

// ClusterAttributes are cluster attributes.
type ClusterAttributes struct {
	ClusterName            string           `json:"cluster_name"`
	SparkVersion           string           `json:"spark_version"`
	SparkConf              S3StorageInfo    `json:"spark_conf"`
	AWSAttributes          AWSAttributes    `json:"aws_attributes"`
	NodeTypeID             string           `json:"node_type_id"`
	DriverNodeTypeID       string           `json:"driver_node_type_id"`
	SSHPublicKeys          []string         `json:"ssh_public_keys"`
	CustomTags             []ClusterTag     `json:"custom_tags"`
	ClusterLogConf         ClusterLogConf   `json:"cluster_log_conf"`
	InitScripts            []InitScriptInfo `json:"init_scripts"`
	SparkEnvVars           SparkEnvPair     `json:"spark_env_vars"`
	AutoterminationMinutes int32            `json:"autotermination_minutes"`
	EnableElasticDisk      bool             `json:"enable_elastic_disk"`
	ClusterSource          AWSAvailability  `json:"cluster_source"`
}

// ClusterSize is a Cluster's size.
type ClusterSize struct {
	NumWorkers *int32     `json:"num_workers"`
	Autoscale  *Autoscale `json:"autoscale"`
}

// EventDetails is the details of an Event.
type EventDetails struct {
	CurrentNumWorkers   int32             `json:"current_num_workers"`
	TargetNumWorkers    int32             `json:"target_num_workers"`
	PreviousAttributes  ClusterAttributes `json:"previous_attributes"`
	Attributes          ClusterAttributes `json:"attributes"`
	PreviousClusterSize ClusterSize       `json:"previous_cluster_size"`
	ClusterSize         ClusterSize       `json:"cluster_size"`
	Cause               string            `json:"cause"`
	Reason              TerminationReason `json:"reason"`
	User                string            `json:"user"`
}

// ClusterEvent is an event that occured on a Cluster.
type ClusterEvent struct {
	ClusterID string       `json:"cluster_id"`
	Timestamp int64        `json:"timestamp"`
	Type      string       `json:"type"` // TODO(daniel): make this a type?
	Details   EventDetails `json:"details"`
}

// ClusterZoneResponse is a reponse for a Cluser zone request.
type ClusterZoneResponse struct {
	Zones       []string `json:"zones"`
	DefaultZone string   `json:"default_zone"`
}

// ClusterEventRequest retrieves events pertaining to a specific cluster.
type ClusterEventRequest struct {
	ClusterID  string     `json:"cluster_id"`
	StartTime  *int64     `json:"start_time"`
	EndTime    *int64     `json:"end_time"`
	Order      *ListOrder `json:"order"`
	EventTypes []string   `json:"event_types"` // TODO(daniel): https://docs.databricks.com/api/latest/clusters.html#clustereventtype
	Offset     int64      `json:"offset"`
	Limit      int64      `json:"limit"`
}

// ClusterEventResponse is a reponse for a ClusterEventRequest.
type ClusterEventResponse struct {
	Events     []string             `json:"events"`
	NextPage   *ClusterEventRequest `json:"next_page"`
	TotalCount int64                `json:"total_count"`
}
