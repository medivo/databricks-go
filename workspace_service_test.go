package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportWorkspaceHelper(t *testing.T) *WorkspaceService {
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
	workspace := badTransportClient.Workspace()
	if workspace == nil {
		t.Fatalf("Workspace returned nil")
	}
	return workspace
}

func non200WorkspaceHelper(t *testing.T) *WorkspaceService {
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
	workspace := non200Client.Workspace()
	if workspace == nil {
		t.Fatalf("Workspace returned nil")
	}
	return workspace
}

func successWorkspaceHelper(
	t *testing.T,
	res []byte,
	code int,
) *WorkspaceService {
	successClient, err := NewClient(
		"test-account",
		ClientHTTPClient(injectedHTTPClient(
			http.Response{
				StatusCode: code,
				Body: nopCloser{
					bytes.NewBuffer(res),
				},
				ContentLength: int64(len(res)),
			},
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if successClient == nil {
		t.Fatalf("NewClient returned nil")
	}
	workspace := successClient.Workspace()
	if workspace == nil {
		t.Fatalf("Workspace returned nil")
	}

	return workspace
}

func Test_WorkspaceService_Delete(t *testing.T) {
	t.Parallel()
	workspace := successWorkspaceHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := workspace.Delete(ctx, "/foo", true)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	err = workspace.Delete(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	err = workspace.Delete(ctx, "/foo", true)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_WorkspaceService_Export(t *testing.T) {
	t.Parallel()
	workspace := successWorkspaceHelper(t, []byte("foo"), http.StatusOK)

	ctx := context.Background()
	exported, err := workspace.Export(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(exported) == 0 {
		t.Fatalf("Expected exported data")
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	_, err = workspace.Export(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	_, err = workspace.Export(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_WorkspaceService_GetStatus(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Path       string
		Language   string
		ObjectType string
	}{
		"/foo",
		"bar",
		"baz",
	})
	if err != nil {
		t.Fatal(err)
	}

	workspace := successWorkspaceHelper(t, res, http.StatusOK)

	ctx := context.Background()
	lang, objType, err := workspace.GetStatus(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(lang) == 0 {
		t.Fatalf("Expected language")
	}
	if len(objType) == 0 {
		t.Fatalf("Expected object type")
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	_, _, err = workspace.GetStatus(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	_, _, err = workspace.GetStatus(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_WorkspaceService_Import(t *testing.T) {
	t.Parallel()
	workspace := successWorkspaceHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := workspace.Import(ctx, "/foo", []byte("bar"), "baz", true, "qux")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	err = workspace.Import(ctx, "/foo", []byte("bar"), "baz", true, "qux")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	err = workspace.Import(ctx, "/foo", []byte("bar"), "baz", true, "qux")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_WorkspaceService_List(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Objects []ObjectInfo
	}{
		[]ObjectInfo{
			ObjectInfo{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	workspace := successWorkspaceHelper(t, res, http.StatusOK)

	ctx := context.Background()
	objects, err := workspace.List(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) == 0 {
		t.Fatalf("Expected objects")
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	_, err = workspace.List(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	_, err = workspace.List(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_WorkspaceService_Mkdirs(t *testing.T) {
	t.Parallel()

	workspace := successWorkspaceHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := workspace.Mkdirs(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	workspace = non200WorkspaceHelper(t)

	err = workspace.Mkdirs(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	workspace = badTransportWorkspaceHelper(t)

	err = workspace.Mkdirs(ctx, "/foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
