package databricks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// WorkspaceService is a service for interacting with the DBFS.
type WorkspaceService struct {
	client Client
}

// Delete removes an object or a directory (and optionally
// recursively deletes all objects in the directory). If
// path does not exist, this call returns an error
// RESOURCE_DOES_NOT_EXIST. If path is a non-empty directory
// and recursive is set to false, this call returns an error
// DIRECTORY_NOT_EMPTY. Object deletion cannot be undone and
// deleting a directory recursively is not atomic.
func (s *WorkspaceService) Delete(
	ctx context.Context,
	path string,
	recursive bool,
) error {
	raw, err := json.Marshal(struct {
		Path      string
		Recursive bool
	}{
		path,
		recursive,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/workspace/delete",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	return nil
}

// Export exports a notebook or contents of an entire directory. If path does
// not exist, this call returns an error RESOURCE_DOES_NOT_EXIST. One can only
// export a directory in DBC format. If the exported data would exceed size
// limit, this call returns an error MAX_NOTEBOOK_SIZE_EXCEEDED. This API does
// not support exporting a library.
func (s *WorkspaceService) Export(
	ctx context.Context,
	path string,
) ([]byte, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/workspace/export",
		nil,
	)
	if err != nil {
		return []byte{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("path", path)
	q.Add("direct_download", "true")
	req.URL.RawQuery = q.Encode()

	res, err := s.client.client.Do(req)
	if err != nil {
		return []byte{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []byte{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}

// GetStatus returns the status of an object or a directory. If path does not
// exist, this call returns an error RESOURCE_DOES_NOT_EXIST. If found it
// returns the language and the object type.
func (s *WorkspaceService) GetStatus(
	ctx context.Context,
	path string,
) (string, string, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/workspace/get-status",
		nil,
	)
	if err != nil {
		return "", "", err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("path", path)
	req.URL.RawQuery = q.Encode()

	res, err := s.client.client.Do(req)
	if err != nil {
		return "", "", err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return "", "", fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	statusRes := struct {
		Path       string
		Language   string
		ObjectType string
	}{}
	err = decoder.Decode(&statusRes)

	return statusRes.Language, statusRes.ObjectType, err
}

// Import imports a notebook or the contents of an entire directory. If path
// already exists and overwrite is set to false, this call returns an error
// RESOURCE_ALREADY_EXISTS. One can only use DBC format to import a directory.
// Example of request, where content is the base64-encoded string of 1+1:
func (s *WorkspaceService) Import(
	ctx context.Context,
	path string,
	content []byte,
	language string,
	overwrite bool,
	format string,
) error {
	raw, err := json.Marshal(struct {
		Path      string
		Content   string
		Format    string
		Language  string
		Overwrite bool
	}{
		path,
		base64.StdEncoding.EncodeToString(content),
		format,
		language,
		overwrite,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/workspace/import",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	return nil
}

// List lists the contents of a directory, or the object if it is not a
// directory. If the input path does not exist, this call returns an error
// RESOURCE_DOES_NOT_EXIST.
func (s *WorkspaceService) List(
	ctx context.Context,
	path string,
) ([]ObjectInfo, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/workspace/list",
		nil,
	)
	if err != nil {
		return []ObjectInfo{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("path", path)
	req.URL.RawQuery = q.Encode()

	res, err := s.client.client.Do(req)
	if err != nil {
		return []ObjectInfo{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []ObjectInfo{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	listRes := struct {
		Objects []ObjectInfo
	}{[]ObjectInfo{}}
	err = decoder.Decode(&listRes)

	return listRes.Objects, err
}

// Mkdirs creates the given directory and necessary parent directories if they
// do not exists. If there exists an object (not a directory) at any prefix of
// the input path, this call returns an error RESOURCE_ALREADY_EXISTS. Note
// that if this operation fails it may have succeeded in creating some of the
// necessary parrent directories.
func (s *WorkspaceService) Mkdirs(
	ctx context.Context,
	path string,
) error {
	raw, err := json.Marshal(struct {
		Path string
	}{
		path,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/workspace/mkdirs",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	return nil
}
