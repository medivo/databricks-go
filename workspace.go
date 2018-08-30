package databricks

// Language is a programming language
type Language string

const (
	Scala  Language = "SCALA"
	Python          = "PYTHON"
	SQL             = "SQL"
	R               = "R"
)

// ObjectType is the type of the object in workspace.
type ObjectType string

const (
	Notebook      ObjectType = "NOTEBOOK"
	Directory                = "DIRECTORY"
	LibraryObject            = "LIBRARY"
)

// ObjectInfo is the information of the object in workspace. It will be
// returned by list and get-status.
type ObjectInfo struct {
	ObjectType ObjectType `json:"object_type"`
	Path       string     `json:"path"`
	Language   Language   `json:"language"`
}
