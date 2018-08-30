package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// ClusterService is used to interact with the Databricks Cluster API.
type ClusterService struct {
	client Client
}

// Create is used to create a new cluster. it returns the
// ID of the newly created cluster.
func (s *ClusterService) Create(
	ctx context.Context,
	createReq *ClusterCreateRequest,
) (string, error) {
	raw, err := json.Marshal(createReq)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/create",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return "", err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return "", err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	createRes := struct {
		ClusterID string `json:"cluster_id"`
	}{}
	err = decoder.Decode(&createRes)

	return createRes.ClusterID, err
}

// Edit is used to edit an existing cluster.
func (s *ClusterService) Edit(
	ctx context.Context,
	editReq *ClusterEditRequest,
) error {
	raw, err := json.Marshal(editReq)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/edit",
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

// Start a terminated Spark cluster given its ID. This is similar to
// createCluster, except:
//
// The previous cluster id and attributes are preserved.
// The cluster starts with the last specified cluster size. If the previous
// cluster was an autoscaling cluster, the current cluster starts with the
// minimum number of nodes.
// If the cluster is not in a TERMINATED state, nothing will happen.
// Clusters launched to run a job cannot be started.
func (s *ClusterService) Start(ctx context.Context, clusterID string) error {
	raw, err := json.Marshal(struct {
		ClusterID string `json:"cluster_id"`
	}{
		clusterID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/start",
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

// Restart a Spark cluster given its id. If the cluster is not in a RUNNING
// state, nothing will happen.
func (s *ClusterService) Restart(ctx context.Context, clusterID string) error {
	raw, err := json.Marshal(struct {
		ClusterID string `json:"cluster_id"`
	}{
		clusterID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/restart",
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

// ResizeWorkers resizes a cluster to have a desired number of workers. This
// will fail unless the cluster is in a RUNNING state.
func (s *ClusterService) ResizeWorkers(
	ctx context.Context,
	clusterID string,
	workers int,
) error {
	raw, err := json.Marshal(struct {
		ClusterID  string `json:"cluster_id"`
		NumWorkers int    `json:"num_workers"`
	}{
		clusterID,
		workers,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/resize",
		bytes.NewBuffer(raw),
	)
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

// ResizeAutoscale resizes a cluster to have a desired number of workers. This
// will fail
// unless the cluster is in a RUNNING state.
func (s *ClusterService) ResizeAutoscale(
	ctx context.Context,
	clusterID string,
	autoscale Autoscale,
) error {
	raw, err := json.Marshal(struct {
		ClusterID string    `json:"cluster_id"`
		Autoscale Autoscale `json:"autoscale"`
	}{
		clusterID,
		autoscale,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/resize",
		bytes.NewBuffer(raw),
	)
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

// Terminate a Spark cluster given its id. The cluster is removed
// asynchronously. Once the termination has completed, the cluster will be in a
// TERMINATED state. If the cluster is already in a TERMINATING or TERMINATED
// state, nothing will happen.
func (s *ClusterService) Terminate(ctx context.Context, clusterID string) error {
	raw, err := json.Marshal(struct {
		ClusterID string `json:"cluster_id"`
	}{
		clusterID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/delete",
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

// Delete permanently deletes a Spark cluster. If the cluster is running, it is
// terminated and its resources are asynchronously removed. If the cluster is
// terminated, then it is immediately removed.
// You cannot perform any action on a permanently deleted cluster and a
// permanently deleted cluster is no longer returned in the cluster list.
func (s *ClusterService) Delete(ctx context.Context, clusterID string) error {
	raw, err := json.Marshal(struct {
		ClusterID string `json:"cluster_id"`
	}{
		clusterID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/permanent-delete",
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

// Get retrieves the information for a cluster given its identifier. Clusters
// can be described while they are running, or up to 30 days after they are
// terminated.
func (s *ClusterService) Get(ctx context.Context, clusterID string) (*ClusterGetResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/clusters/get",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("cluster_id", clusterID)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(
			"Failed to return a 2XX response (%d): %s", res.StatusCode, body)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	var getRes ClusterGetResponse
	err = decoder.Decode(&getRes)

	return &getRes, err
}

// Pin ensures that the cluster is always returned by the List
// API. Pinning a cluster that is already pinned has no effect.
func (s *ClusterService) Pin(ctx context.Context, clusterID string) error {
	raw, err := json.Marshal(struct {
		ClusterID string `json:"cluster_id"`
	}{
		clusterID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/pin",
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

// Unpin will allow the cluster to eventually be removed from the
// list returned by the List API. Unpinning a cluster that is not pinned has no
// effect.
func (s *ClusterService) Unpin(ctx context.Context, clusterID string) error {
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/unpin",
		nil,
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

// List returns information about all pinned clusters, currently active
// clusters, up to 70 of the most recently terminated interactive clusters in
// the past 30 days, and up to 30 of the most recently terminated job clusters
// in the past 30 days. For example, if there is 1 pinned cluster, 4 active
// clusters, 45 terminated interactive clusters in the past 30 days, and 50
// terminated job clusters in the past 30 days, then this API returns the 1
// pinned cluster, 4 active clusters, all 45 terminated interactive clusters,
// and the 30 most recently terminated job clusters.
func (s *ClusterService) List(ctx context.Context) ([]ClusterInfo, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/clusters/list",
		nil,
	)
	if err != nil {
		return []ClusterInfo{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []ClusterInfo{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []ClusterInfo{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	listRes := struct {
		Clusters []ClusterInfo `json:"clusters"` // TODO(daniel) the API is wrong
	}{[]ClusterInfo{}}
	err = decoder.Decode(&listRes)

	return listRes.Clusters, err

}

// Zones Returns a list of availability zones where clusters can be created in
// (ex: us-west-2a). These zones can be used to launch a cluster.
func (s *ClusterService) Zones(
	ctx context.Context,
) (*ClusterZoneResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/clusters/list-zones",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return nil, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	var zoneRes ClusterZoneResponse
	err = decoder.Decode(&zoneRes)

	return &zoneRes, err

}

// NodeTypes returns a list of supported Spark node types. These node types can
// be used to launch a cluster.
func (s *ClusterService) NodeTypes(ctx context.Context) ([]NodeType, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/clusters/list-node-types",
		nil,
	)
	if err != nil {
		return []NodeType{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []NodeType{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []NodeType{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	nodeTypes := []NodeType{}
	err = decoder.Decode(&nodeTypes)

	return nodeTypes, err
}

// SparkVersions returns the list of available Spark versions. These versions
// can be used to launch a cluster.
func (s *ClusterService) SparkVersions(
	ctx context.Context,
) ([]SparkNodeAwsAttributes, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/clusters/spark-versions",
		nil,
	)
	if err != nil {
		return []SparkNodeAwsAttributes{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []SparkNodeAwsAttributes{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []SparkNodeAwsAttributes{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	attrs := struct {
		Versions []SparkNodeAwsAttributes
	}{
		[]SparkNodeAwsAttributes{},
	}
	err = decoder.Decode(&attrs)
	return attrs.Versions, err
}

// Events retrieves a list of events about the activity of a cluster. This API
// is paginated. If there are more events to read, the response includes all
// the parameters necessary to request the next page of events.
func (s *ClusterService) Events(
	ctx context.Context,
	eventReq *ClusterEventRequest,
) (*ClusterEventResponse, error) {
	rawData, err := json.Marshal(eventReq)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/clusters/events",
		bytes.NewBuffer(rawData),
	)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return nil, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	var eventRes ClusterEventResponse
	if err := decoder.Decode(&eventRes); err != nil {
		return nil, err
	}

	return &eventRes, nil
}
