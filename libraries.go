package databricks

// MavenLibrary is a Maven library.
type MavenLibrary struct {
	Coordinates string   `json:"coordinates"`
	Repo        *string  `json:"repo"`
	Exclusions  []string `json:"exclusions"`
}

// PythonPyPiLibrary is a PyPil library
type PythonPyPiLibrary struct {
	Package string  `json:"package"`
	Repo    *string `json:"repo"`
}

// RCranLibrary is an R library
type RCranLibrary struct {
	Package string  `json:"package"`
	Repo    *string `json:"repo"`
}

// Library is a library that is run.
type Library struct {
	Jar   *string            `json:"jar"`
	Egg   *string            `json:"egg"`
	Whl   *string            `json:"whl"`
	Pypi  *PythonPyPiLibrary `json:"pypi"`
	Maven *MavenLibrary      `json:"maven"`
	Cran  *RCranLibrary      `json:"cran"`
}

// LibraryFullStatus is the status of the library on a specific cluster.
type LibraryFullStatus struct {
	Library                 Library  `json:"library"`
	Status                  string   `json:"status"` // TODO(daniel)
	Messages                []string `json:"messages"`
	IsLibraryForAllClusters bool     `json:"is_library_for_all_clusters"`
}

// ClusterLibraryStatuses contains the statuses for a Cluster library.
type ClusterLibraryStatuses struct {
	ClusterID       string              `json:"cluster_id"`
	LibraryStatuses []LibraryFullStatus `json:"library_statuses"`
}
