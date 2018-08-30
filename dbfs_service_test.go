package databricks

import (
	"bytes"
	"context"
	"net/http"
	"testing"
)

func badTransportDBFSHelper(t *testing.T) *DBFSService {
	badTransportClient, err := NewClient(
		"test-account",
		ClientHTTPClient(BadTransportHTTPClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	if badTransportClient == nil {
		t.Fatalf("NewClient returned nil")
	}
	dbfs := badTransportClient.DBFS()
	if dbfs == nil {
		t.Fatalf("DBFS returned nil")
	}
	return dbfs
}

func non200DBFSHelper(t *testing.T) *DBFSService {
	non200Client, err := NewClient(
		"test-account",
		ClientHTTPClient(Non200HTTPClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	if non200Client == nil {
		t.Fatalf("NewClient returned nil")
	}
	dbfs := non200Client.DBFS()
	if dbfs == nil {
		t.Fatalf("DBFS returned nil")
	}
	return dbfs
}

func successDBFSHelper(
	t *testing.T,
	res []byte,
	code int,
) *DBFSService {
	successClient, err := NewClient(
		"test-account",
		ClientHTTPClient(injectedHTTPClient(
			http.Response{
				StatusCode: code,
				Body: nopCloser{
					bytes.NewBuffer(res),
				},
			},
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if successClient == nil {
		t.Fatalf("NewClient returned nil")
	}
	dbfs := successClient.DBFS()
	if dbfs == nil {
		t.Fatalf("DBFS returned nil")
	}

	return dbfs
}

func Test_DBFSService_AddBlock(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	err := dbfs.AddBlock(ctx, int64(123), []byte{})
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.AddBlock(ctx, int64(123), []byte{})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.AddBlock(ctx, int64(123), []byte{})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Close(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := dbfs.Close(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.Close(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.Close(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Create(t *testing.T) {
	t.Parallel()
	res := []byte(`{"handle": 123}`)

	dbfs := successDBFSHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	id, err := dbfs.Create(ctx, "/foo", true)
	if err != nil {
		t.Fatal(err)
	}
	if id != int64(123) {
		t.Fatalf("Expectd ID 123")
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	id, err = dbfs.Create(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	id, err = dbfs.Create(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Delete(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := dbfs.Delete(ctx, "/foo", true)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.Delete(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.Delete(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_GetStatus(t *testing.T) {
	t.Parallel()
	res := []byte(`{"path":"/foo","is_dir":false,"file_size":1234}`)

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	isDir, fileSize, err := dbfs.GetStatus(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}
	if fileSize <= int64(0) {
		t.Fatalf("Expected file size > 0")
	}
	if isDir {
		t.Fatalf("Expected IsDir to return False")
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	isDir, fileSize, err = dbfs.GetStatus(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	isDir, fileSize, err = dbfs.GetStatus(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_List(t *testing.T) {
	t.Parallel()
	res := []byte(`{"files":[{"path":"/foo","is_dir":false,"file_size":123}]}`)

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	files, err := dbfs.List(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("Expected a non empty []FileInfo")
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	_, err = dbfs.List(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	_, err = dbfs.List(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Mkdirs(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := dbfs.Mkdirs(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.Mkdirs(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.Mkdirs(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Move(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := dbfs.Move(ctx, "/foo", "/bar")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.Move(ctx, "/foo", "/bar")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.Move(ctx, "/foo", "/bar")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Put(t *testing.T) {
	t.Parallel()
	res := []byte{}

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := dbfs.Put(ctx, "/foo", []byte{}, true)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	err = dbfs.Put(ctx, "/foo", []byte{}, true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	err = dbfs.Put(ctx, "/foo", []byte{}, true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DBFSService_Read(t *testing.T) {
	t.Parallel()
	res := []byte(`{"bytes_read":123,"data":"aGVsbG8gd29ybGQK"}`)

	dbfs := successDBFSHelper(t, res, http.StatusOK)

	ctx := context.Background()
	bytesRead, data, err := dbfs.Read(ctx, "/foo", int64(0), int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if bytesRead <= 0 {
		t.Fatalf("Expected bytes read to be positive")
	}
	if len(data) == 0 {
		t.Fatalf("Expected data")
	}

	// Non 200 test
	dbfs = non200DBFSHelper(t)

	bytesRead, data, err = dbfs.Read(ctx, "/foo", int64(0), int64(1))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	dbfs = badTransportDBFSHelper(t)

	bytesRead, data, err = dbfs.Read(ctx, "/foo", int64(0), int64(1))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
