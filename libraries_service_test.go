package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportLibrariesHelper(t *testing.T) *LibrariesService {
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
	libraries := badTransportClient.Libraries()
	if libraries == nil {
		t.Fatalf("Libraries returned nil")
	}
	return libraries
}

func non200LibrariesHelper(t *testing.T) *LibrariesService {
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
	libraries := non200Client.Libraries()
	if libraries == nil {
		t.Fatalf("Libraries returned nil")
	}
	return libraries
}

func successLibrariesHelper(
	t *testing.T,
	res []byte,
	code int,
) *LibrariesService {
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
	libraries := successClient.Libraries()
	if libraries == nil {
		t.Fatalf("Libraries returned nil")
	}

	return libraries
}

func Test_LibrariesService_AllClusterStatuses(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Statuses []ClusterLibraryStatuses
	}{
		[]ClusterLibraryStatuses{
			ClusterLibraryStatuses{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	libraries := successLibrariesHelper(t, res, http.StatusOK)

	ctx := context.Background()
	statuses, err := libraries.AllClusterStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) == 0 {
		t.Fatalf("Expected more than 0 statuses")
	}

	// Non 200 test
	libraries = non200LibrariesHelper(t)

	_, err = libraries.AllClusterStatuses(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	libraries = badTransportLibrariesHelper(t)

	_, err = libraries.AllClusterStatuses(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_LibrariesService_ClusterStatus(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Statuses []LibraryFullStatus
	}{
		[]LibraryFullStatus{
			LibraryFullStatus{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	libraries := successLibrariesHelper(t, res, http.StatusOK)

	ctx := context.Background()
	statuses, err := libraries.ClusterStatus(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) == 0 {
		t.Fatalf("Expected more than 0 statuses")
	}

	// Non 200 test
	libraries = non200LibrariesHelper(t)

	_, err = libraries.ClusterStatus(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	libraries = badTransportLibrariesHelper(t)

	_, err = libraries.ClusterStatus(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_LibrariesService_Install(t *testing.T) {
	t.Parallel()
	libraries := successLibrariesHelper(t, []byte{}, http.StatusOK)

	libs := []Library{}
	ctx := context.Background()
	err := libraries.Install(ctx, "cluster-123", libs)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	libraries = non200LibrariesHelper(t)

	err = libraries.Install(ctx, "cluster-123", libs)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	libraries = badTransportLibrariesHelper(t)

	err = libraries.Install(ctx, "cluster-123", libs)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_LibrariesService_Uninstall(t *testing.T) {
	t.Parallel()
	libraries := successLibrariesHelper(t, []byte{}, http.StatusOK)

	libs := []Library{}
	ctx := context.Background()
	err := libraries.Uninstall(ctx, "cluster-123", libs)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	libraries = non200LibrariesHelper(t)

	err = libraries.Uninstall(ctx, "cluster-123", libs)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	libraries = badTransportLibrariesHelper(t)

	err = libraries.Uninstall(ctx, "cluster-123", libs)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
