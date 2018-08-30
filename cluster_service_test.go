package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportClusterHelper(t *testing.T) *ClusterService {
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
	cluster := badTransportClient.Cluster()
	if cluster == nil {
		t.Fatalf("Cluster returned nil")
	}
	return cluster
}

func non200ClusterHelper(t *testing.T) *ClusterService {
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
	cluster := non200Client.Cluster()
	if cluster == nil {
		t.Fatalf("Cluster returned nil")
	}
	return cluster
}

func successClusterHelper(
	t *testing.T,
	res []byte,
	code int,
) *ClusterService {
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
	cluster := successClient.Cluster()
	if cluster == nil {
		t.Fatalf("Cluster returned nil")
	}

	return cluster
}

func Test_ClusterService_Create(t *testing.T) {
	t.Parallel()
	res := []byte(`{"cluster_id":"expected-123"}`)

	cluster := successClusterHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	createReq := &ClusterCreateRequest{}
	clusterID, err := cluster.Create(ctx, createReq)
	if err != nil {
		t.Fatal(err)
	}
	if len(clusterID) == 0 {
		t.Fatalf("ClusterID returned empty")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	clusterID, err = cluster.Create(ctx, createReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	clusterID, err = cluster.Create(ctx, createReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Edit(t *testing.T) {
	t.Parallel()
	res := []byte(`{"cluster_id":"expected-123"}`)
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	editReq := &ClusterEditRequest{}
	err := cluster.Edit(ctx, editReq)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Edit(ctx, editReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Edit(ctx, editReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Start(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Start(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Start(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Start(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Restart(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Restart(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Restart(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Restart(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_ResizeWorkers(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.ResizeWorkers(ctx, "cluster-123", 2)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.ResizeWorkers(ctx, "cluster-123", 2)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.ResizeWorkers(ctx, "cluster-123", 2)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_ResizeAutoscale(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.ResizeAutoscale(ctx, "cluster-123", Autoscale{2, 4})
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.ResizeAutoscale(ctx, "cluster-123", Autoscale{2, 4})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.ResizeAutoscale(ctx, "cluster-123", Autoscale{2, 4})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Terminate(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Terminate(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Terminate(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Terminate(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Delete(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Delete(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Delete(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Delete(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Get(t *testing.T) {
	res, err := json.Marshal(&ClusterGetResponse{})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	getRes, err := cluster.Get(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}
	if getRes == nil {
		t.Fatalf("Response should not be nil")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	getRes, err = cluster.Get(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	getRes, err = cluster.Get(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Pin(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Pin(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Pin(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Pin(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Unpin(t *testing.T) {
	cluster := successClusterHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := cluster.Unpin(ctx, "cluster-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	err = cluster.Unpin(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	err = cluster.Unpin(ctx, "cluster-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_List(t *testing.T) {
	res, err := json.Marshal(struct {
		Clusters []ClusterInfo
	}{
		[]ClusterInfo{
			ClusterInfo{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	attrs, err := cluster.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) < 1 {
		t.Fatalf("Expected to return AWSAttributes")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	attrs, err = cluster.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	attrs, err = cluster.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Zones(t *testing.T) {
	res, err := json.Marshal(&ClusterZoneResponse{})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	zoneRes, err := cluster.Zones(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if zoneRes == nil {
		t.Fatalf("Expected a ClusterZoneResponse")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	zoneRes, err = cluster.Zones(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	zoneRes, err = cluster.Zones(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_NodeTypes(t *testing.T) {
	res, err := json.Marshal([]NodeType{
		NodeType{},
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	nodeTypes, err := cluster.NodeTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodeTypes) < 1 {
		t.Fatalf("Expected to return NodeType")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	nodeTypes, err = cluster.NodeTypes(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	nodeTypes, err = cluster.NodeTypes(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_SparkVersions(t *testing.T) {
	res, err := json.Marshal(struct {
		Versions []SparkNodeAwsAttributes
	}{
		[]SparkNodeAwsAttributes{
			SparkNodeAwsAttributes{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	ctx := context.Background()
	sparkNodeAwsAttrs, err := cluster.SparkVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sparkNodeAwsAttrs) < 1 {
		t.Fatalf("Expected to return NodeType")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	sparkNodeAwsAttrs, err = cluster.SparkVersions(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	sparkNodeAwsAttrs, err = cluster.SparkVersions(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ClusterService_Events(t *testing.T) {
	res, err := json.Marshal(&ClusterEventResponse{})
	if err != nil {
		t.Fatal(err)
	}
	cluster := successClusterHelper(t, res, http.StatusOK)

	eventReq := &ClusterEventRequest{}

	ctx := context.Background()
	eventRes, err := cluster.Events(ctx, eventReq)
	if err != nil {
		t.Fatal(err)
	}
	if eventRes == nil {
		t.Fatalf("Expected a ClusterEventResponse")
	}

	// Non 200 test
	cluster = non200ClusterHelper(t)

	eventRes, err = cluster.Events(ctx, eventReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	cluster = badTransportClusterHelper(t)

	eventRes, err = cluster.Events(ctx, eventReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
