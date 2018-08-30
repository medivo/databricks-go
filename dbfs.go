package databricks

// FileInfo is file info for DBFS.
type FileInfo struct {
	Path     string
	IsDir    bool
	FileSize int64
}
