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

// DBFSService is a service for interacting with the DBFS.
type DBFSService struct {
	client Client
}

// AddBlock adds to a block of DBFS AND automatically base64 encodes the data.
func (s *DBFSService) AddBlock(
	ctx context.Context,
	handle int64,
	data []byte,
) error {
	raw, err := json.Marshal(struct {
		Data   string `json:"data"`
		Handle int64  `json:"handle"`
	}{
		base64.StdEncoding.EncodeToString(data),
		handle,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/add-block",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	res, err := s.client.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}

	return nil
}

// Close will close the stream specified by the input handle. If the handle
// does not exist, this call will throw an exception with
// RESOURCE_DOES_NOT_EXIST.
func (s *DBFSService) Close(
	ctx context.Context,
	handle int64,
) error {
	raw, err := json.Marshal(struct {
		Handle int64 `json:"handle"`
	}{
		handle,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/close",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Create Opens a stream to write to a file and returns a handle to this
// stream. There is a 10 minute idle timeout on this handle. If a file or
// directory already exists on the given path and overwrite is set to false,
// this call will throw an exception with RESOURCE_ALREADY_EXISTS. A typical
// workflow for file upload would be:
//
// Issue a create call and get a handle.
// Issue one or more add-block calls with the handle you have.
// Issue a close call with the handle you have.
func (s *DBFSService) Create(
	ctx context.Context,
	path string,
	overwrite bool,
) (int64, error) {
	raw, err := json.Marshal(struct {
		Path      string `json:"path"`
		Overwrite bool   `json:"overwrite"`
	}{
		path,
		overwrite,
	})
	if err != nil {
		return -1, err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/create",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return -1, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return -1, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return -1, err
		}
		return -1, fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	createRes := struct {
		Handle int64
	}{}
	err = decoder.Decode(&createRes)

	return createRes.Handle, err
}

// Delete the file or directory (optionally recursively
// delete all files in the directory). This call will throw
// an exception with IO_ERROR if the path is a non-empty
// directory and recursive is set to false or on other
// similar errors.
func (s *DBFSService) Delete(
	ctx context.Context,
	path string,
	recursive bool,
) error {
	raw, err := json.Marshal(struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}{
		path,
		recursive,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/delete",
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
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}

	return nil
}

// GetStatus returns the file information of a file or
// directory. If the file or directory does not exist, this
// call will throw an exception with
// RESOURCE_DOES_NOT_EXIST.
func (s *DBFSService) GetStatus(
	ctx context.Context,
	path string,
) (bool, int64, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/dbfs/get-status",
		nil,
	)
	if err != nil {
		return false, -1, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("path", path)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return false, -1, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return false, -1, err
		}
		return false, -1, fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	getRes := struct {
		Path     string `json:"path"`
		IsDir    bool   `json:"is_dir"`
		FileSize int64  `json:"file_size"`
	}{}
	err = decoder.Decode(&getRes)

	return getRes.IsDir, getRes.FileSize, err
}

// List the contents of a directory, or details of the file. If the file or
// directory does not exist, this call will throw an exception with
// RESOURCE_DOES_NOT_EXIST.
func (s *DBFSService) List(
	ctx context.Context,
	path string,
) ([]FileInfo, error) {
	raw, err := json.Marshal(struct {
		Path string `json:"path"`
	}{
		path,
	})
	if err != nil {
		return []FileInfo{}, err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/get-status",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return []FileInfo{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []FileInfo{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []FileInfo{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	listRes := struct {
		Files []FileInfo
	}{
		[]FileInfo{},
	}
	err = decoder.Decode(&listRes)

	return listRes.Files, nil
}

// Mkdirs creates the given directory and necessary parent directories if they
// do not exist. If there exists a file (not a directory) at any prefix of the
// input path, this call will throw an exception with RESOURCE_ALREADY_EXISTS.
// Note that if this operation fails it may have succeeded in creating some of
// the necessary parent directories.
func (s *DBFSService) Mkdirs(
	ctx context.Context,
	path string,
) error {
	raw, err := json.Marshal(struct {
		Path string `json:"path"`
	}{
		path,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/mkdirs",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Move a file from one location to another location within DBFS. If the source
// file does not exist, this call will throw an exception with
// RESOURCE_DOES_NOT_EXIST. If there already exists a file in the destination
// path, this call will throw an exception with RESOURCE_ALREADY_EXISTS. If the
// given source path is a directory, this call will always recursively move all
// files.
func (s *DBFSService) Move(
	ctx context.Context,
	src, dest string,
) error {
	raw, err := json.Marshal(struct {
		SourcePath      string `json:"source_path"`
		DestinationPath string `json:"destination_path"`
	}{
		src,
		dest,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/move",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Put uploads a file through the use of multipart form post. It is mainly used
// for streaming uploads, but can also be used as a convenient single call for
// data upload
func (s *DBFSService) Put(
	ctx context.Context,
	path string,
	content []byte,
	overwrite bool,
) error {
	raw, err := json.Marshal(struct {
		Path      string `json:"path"`
		Content   []byte `json:"content"`
		Overwrite bool   `json:"overwrite"`
	}{
		path,
		content,
		overwrite,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/dbfs/put",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Read returns the contents of a file. If the file does not exist, this call
// will throw an exception with RESOURCE_DOES_NOT_EXIST. If the path is a
// directory, the read length is negative, or if the offset is negative, this
// call will throw an exception with INVALID_PARAMETER_VALUE. If the read
// length exceeds 1 MB, this call will throw an exception with
// MAX_READ_SIZE_EXCEEDED. If offset + length exceeds the number of bytes in a
// file, we will read contents until the end of file.
func (s *DBFSService) Read(
	ctx context.Context,
	path string,
	offset, length int64,
) (int64, []byte, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/dbfs/read",
		nil,
	)
	if err != nil {
		return -1, []byte{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("path", path)
	q.Add("offset", fmt.Sprintf("%d", offset))
	q.Add("length", fmt.Sprintf("%d", length))
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return -1, []byte{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return -1, []byte{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	readRes := struct {
		BytesRead int64  `json:"bytes_read"`
		Data      []byte `json:"data"`
	}{
		-1,
		[]byte{},
	}
	err = decoder.Decode(&readRes)

	return readRes.BytesRead, readRes.Data, err
}
