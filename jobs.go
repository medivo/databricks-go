package databricks

import "time"

// View is a view of a job.
type View struct {
	Content string `json:"content"`
	Name    string `json:"name"`
	Type    string `json:"type"`
}

// RunState is a job run state.
type RunState struct {
	LifeCycleState string `json:"life_cycle_state"` // TODO(daniel) make this an enum
	ResultState    string `json:"result_state"`     // TODO(daniel) make this an enum
	StateMessage   string `json:"state_message"`
}

// JobTask is a job task.
type JobTask struct {
	NotebookTask    *NotebookTask    `json:"notebook_task,omitempty"`
	SparkJarTask    *SparkJarTask    `json:"spark_jar_task,omitempty"`
	SparkPythonTask *SparkPythonTask `json:"spark_python_task,omitempty"`
	SparkSubmitTask *SparkSubmitTask `json:"spark_submit_task,omitempty"`
}

// JobRunGetResponse is the metadata of a run.
type JobRunGetResponse struct {
	JobID                int64           `json:"job_id"`
	RunID                int64           `json:"run_id"`
	NumberInJob          int64           `json:"number_in_job"`
	OriginalAttemptRunID int64           `json:"original_attempt_run_id"`
	State                RunState        `json:"state"`
	Schedule             *CronSchedule   `json:"schedule"`
	Task                 JobTask         `json:"task"`
	ClusterSpec          ClusterSpec     `json:"cluster_spec"`
	ClusterInstance      ClusterInstance `json:"cluster_instance"`
	OverridingParameters RunParameters   `json:"overriding_parameters"`
	StartTime            int64           `json:"start_time"`
	SetupDuration        int64           `json:"setup_duration"`
	ExecutionDuration    int64           `json:"execution_duration"`
	CleanupDuration      int64           `json:"cleanup_duration"`
	Trigger              string          `json:"trigger"` // TODO(daniel) enum this?
	CreatorUserName      string          `json:"creator_user_name"`
	RunPageurl           *string         `json:"run_pageurl"`
}

// JobRunListRequest is used to request Run information.
type JobRunListRequest struct {
	ActiveOnly   *bool `json:"active_only,omitempty"`
	CompleteOnly *bool `json:"complete_only,omitempty"`
	JobID        int64 `json:"job_id"`
	Offset       int   `json:"offset"`
	Limit        int   `json:"limit"`
}

// JobSubmitSettings is used to configure a job for submission.
type JobSubmitSettings struct {
	ExistingClusterID *string          `json:"existing_cluster_id"`
	NewCluster        *NewCluster      `json:"new_cluster"`
	NotebookTask      *NotebookTask    `json:"notebook_task"`
	SparkJarTask      *SparkJarTask    `json:"spark_jar_task"`
	SparkPythonTask   *SparkPythonTask `json:"spark_python_task"`
	SparkSubmitTask   *SparkSubmitTask `json:"spark_submit_task"`
	RunName           *string          `json:"run_name"`
	Libraries         []Library        `json:"libraries"`
	TimeoutSeconds    *int32           `json:"timeout_seconds"`
}

// JobRunNowSettings is used to configure a job for the RunNow API.
type JobRunNowSettings struct {
	JobID             int64       `json:"job_id"`
	JarParams         []string    `json:"jar_params,omitempty"`
	NotebookParams    []ParamPair `json:"notebook_params,omitempty"`
	PythonParams      []string    `json:"python_params,omitempty"`
	SparkSubmitParams []string    `json:"spark_submit_params,omitempty"`
}

// JobGetResponse is returned when getting a job info.
type JobGetResponse struct {
	JobID           int64       `json:"job_id"`
	CreatorUserName string      `json:"creator_user_name"`
	Settings        JobSettings `json:"settings"`
	CreatedTime     int64       `json:"created_time"`
}

// JobEmailNotifications is set of email addresses that will be notified when runs of this job begin or complete as well as when this job is deleted.
type JobEmailNotifications struct {
	OnStart   []string `json:"on_start"`
	OnSuccess []string `json:"on_success"`
	OnFailure []string `json:"on_failure"`
}

// JobCreateRequest is used for creating new jobs.
type JobCreateRequest struct {
	ExistingClusterID      *string                `json:"existing_cluster_id,omitempty"`
	NewCluster             *NewCluster            `json:"new_cluster,omitempty"`
	NotebookTask           *NotebookTask          `json:"notebook_task,omitempty"`
	SparkJarTask           *SparkJarTask          `json:"spark_jar_task,omitempty"`
	SparkPythonTask        *SparkPythonTask       `json:"spark_python_task,omitempty"`
	SparkSubmitTask        *SparkSubmitTask       `json:"spark_submit_task,omitempty"`
	Name                   string                 `json:"name"`
	Libraries              []Library              `json:"libraries"`
	EmailNotifications     *JobEmailNotifications `json:"email_notifications,omitempty"`
	TimeoutSeconds         *int32                 `json:"timeout_seconds,omitempty"`
	MaxRetries             *int32                 `json:"max_retries,omitempty"`
	MinRetryIntervalMillis *int32                 `json:"min_retry_interval_millis,omitempty"`
	RetryOnTimeout         *bool                  `json:"retry_on_timeout,omitempty"`
	Schedule               *CronSchedule          `json:"schedule,omitempty"`
	MaxConcurrentRuns      *int32                 `json:"max_concurrent_runs,omitempty"`
}

// ClusterInstance identifiers for the cluster and Spark context used by a run.
// These two values together identify an execution context across all time.
type ClusterInstance struct {
	ClusterID      string `json:"cluster_id"`
	SparkContextID string `json:"spark_context_id"`
}

// CronSchedule is a cron schedule.
type CronSchedule struct {
	QuartzCronExpression string `json:"quartz_cron_expression"`
	TimezoneID           string `json:"timezone_id"`
}

// NewCluster is settings for a new Cluster.
type NewCluster struct {
	NumWorkers             *int32           `json:"num_workers"`
	Autoscale              *Autoscale       `json:"autoscale"`
	ClusterName            string           `json:"cluster_name"`
	SparkVersion           string           `json:"spark_version"`
	SparkConf              *S3StorageInfo   `json:"spark_conf"`
	AWSAttributes          AWSAttributes    `json:"aws_attributes"`
	NodeTypeID             string           `json:"node_type_id"`
	DriverNodeTypeID       string           `json:"driver_node_type_id"`
	SSHPublicKeys          []string         `json:"ssh_public_keys"`
	CustomTags             []ClusterTag     `json:"custom_tags"`
	ClusterLogConf         ClusterLogConf   `json:"cluster_log_conf"`
	InitScripts            []InitScriptInfo `json:"init_scripts"`
	SparkEnvVars           SparkEnvPair     `json:"spark_env_vars"`
	AutoterminationMinutes *int32           `json:"autotermination_minutes"`
	EnableElasticDisk      bool             `json:"enable_elastic_disk"`
}

// NotebookOutput is the output of a Notebook.
type NotebookOutput struct {
	Result string `json:"result"`
}

// ParamPair is a key value pair of Notebook parameters.
type ParamPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NotebookTask is a Notebook task.
type NotebookTask struct {
	NotebookPath   string      `json:"notebook_path"`
	BaseParameters []ParamPair `json:"base_parameters"`
}

// SparkJarTask is a Spark jar run.
type SparkJarTask struct {
	JarURI        string   `json:"jar_uri"`
	MainClassName string   `json:"main_class_name"`
	Parameters    []string `json:"parameters"`
}

// SparkPythonTask is a Spark python task.
type SparkPythonTask struct {
	PythonFile string   `json:"python_file"`
	Parameters []string `json:"parameters"`
}

// SparkSubmitTask is a submit Spark task.
type SparkSubmitTask struct {
	Parameters []string `json:"parameters"`
}

// JobSettings are job settings.
type JobSettings struct {
	ExistingClusterID      *string                `json:"existing_cluster_id"`
	NewCluster             *NewCluster            `json:"new_cluster"`
	NotebookTask           *NotebookTask          `json:"notebook_task"`
	SparkJarTask           *SparkJarTask          `json:"spark_jar_task"`
	SparkPythonTask        *SparkPythonTask       `json:"spark_python_task"`
	SparkSubmitTask        *SparkSubmitTask       `json:"spark_submit_task"`
	Name                   *string                `json:"name"`
	Libraries              []Library              `json:"libraries"`
	EmailNotifications     *JobEmailNotifications `json:"email_notifications,omitempty"`
	TimeoutSeconds         *int32                 `json:"timeout_seconds"`
	MaxRetries             *int32                 `json:"max_retries"`
	MinRetryIntervalMillis *int32                 `json:"min_retry_interval_millis"`
	RetryOnTimeout         *bool                  `json:"retry_on_timeout"`
	Schedule               *CronSchedule          `json:"schedule"`
	MaxConcurrentRuns      *int32                 `json:"max_concurrent_runs"`
}

// Job is a job.
type Job struct {
	JobID           int64       `json:"job_id"`
	CreatorUserName string      `json:"creator_user_name"`
	Settings        JobSettings `json:"settings"`
	CreatedTime     time.Time   `json:"created_time"`
}

// ClusterSpec is a Cluster specification.
type ClusterSpec struct {
	ExistingClusterID *string     `json:"existing_cluster_id"`
	NewCluster        *NewCluster `json:"new_cluster"`
	Libraries         []Library
}

// RunParameters are parameters for this run. Only one of jar_params,
// python_params or notebook_params should be specified in the run-now request,
// depending on the type of job task. Jobs with jar task or python task take a
// list of position-based parameters, and jobs with notebook tasks take a key
// value map.
type RunParameters struct {
	JarParams         []string    `json:"jar_params"`
	NotebookParams    []ParamPair `json:"notebook_params"`
	PythonParams      []string    `json:"python_params"`
	SparkSubmitParams []string    `json:"spark_submit_params"`
}

// Run is all the information about a run except for its
// output. The output can be retrieved separately with the
// getRunOutput method.
type Run struct {
	JobID                int64        `json:"job_id"`
	RunID                int64        `json:"run_id"`
	CreatorUserName      string       `json:"creator_user_name"`
	NumberInJob          int64        `json:"number_in_job"`
	OriginalAttemptRunid int64        `json:"original_attempt_runid"`
	State                string       `json:"state"` // TODO(daniel)
	Schedule             CronSchedule `json:"schedule"`
	// Task                 JobTask         `json:"task"` // TODO(daniel)
	ClusterSpec          ClusterSpec     `json:"cluster_spec"`
	ClusterInstance      ClusterInstance `json:"cluster_instance"`
	OverridingParameters RunParameters   `json:"overriding_parameters"`
	StartTime            time.Time       `json:"start_time"`
	SetupDuration        int64           `json:"setup_duration"`
	ExecutionDuration    int64           `json:"execution_duration"`
	CleanupDuration      int64           `json:"cleanup_duration"`
	//Trigger              TriggerType     `json:"trigger"` // TODO(daniel)
}
