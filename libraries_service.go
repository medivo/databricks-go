package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// LibrariesService is a service for interacting with the DBFS.
type LibrariesService struct {
	client Client
}

// AllClusterStatuses gets the status of all libraries on all clusters. A
// status will be available for all libraries installed on clusters via the API
// or the libraries UI as well as libraries set to be installed on all clusters
// via the libraries UI.  If a library has been set to be installed on all
// clusters, is_library_for_all_clusters will be true, even if the library was
// also installed on this specific cluster.
func (s *LibrariesService) AllClusterStatuses(
	ctx context.Context,
) ([]ClusterLibraryStatuses, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/libraries/all-cluster-statuses",
		nil,
	)
	if err != nil {
		return []ClusterLibraryStatuses{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []ClusterLibraryStatuses{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []ClusterLibraryStatuses{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	statusRes := struct {
		Statuses []ClusterLibraryStatuses
	}{[]ClusterLibraryStatuses{}}
	err = decoder.Decode(&statusRes)

	return statusRes.Statuses, err
}

// ClusterStatus get the status of libraries on a cluster. A status will be
// available for all libraries installed on the cluster via the API or the
// libraries UI as well as libraries set to be installed on all clusters via
// the libraries UI. If a library has been set to be installed on all clusters,
// is_library_for_all_clusters will be true, even if the library was also
// installed on the cluster.
func (s *LibrariesService) ClusterStatus(
	ctx context.Context,
	clusterID string,
) ([]LibraryFullStatus, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/libraries/cluster-status",
		nil,
	)
	if err != nil {
		return []LibraryFullStatus{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("cluster_id", clusterID)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return []LibraryFullStatus{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []LibraryFullStatus{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	statusRes := struct {
		Statuses []LibraryFullStatus
	}{[]LibraryFullStatus{}}
	err = decoder.Decode(&statusRes)

	return statusRes.Statuses, err
}

// Install libraries on a cluster. The installation is asynchronous -
// it happens in the background after the completion of this request. The
// actual set of libraries to be installed on a cluster is the union of the
// libraries specified via this method and the libraries set to be installed on
// all clusters via the libraries UI.
//
// Installing a wheel library on clusters running Databricks Runtime 4.2 or
// higher is like running the pip command against the wheel file directly on
// driver and executors. All the dependencies specified in the library setup.py
// file are installed and this requires the library name to satisfy the wheel
// file name convention. The installation on the executors happens only when a
// new task is launched and the installation order is nondeterministic if there
// are multiple wheel files to be installed by the same task launching. To get
// a deterministic installation order, create a zip file with suffix
// .wheelhouse.zip that includes all the wheel files.
//
// Installing a wheel library on clusters running Databricks Runtime lower than
// 4.2 adds the file to the PYTHONPATH variable, without installing the
// dependencies.
//
// CRAN libraries can be installed only on clusters running Databricks Runtime
// 3.2 and above.
func (s *LibrariesService) Install(
	ctx context.Context,
	clusterID string,
	libraries []Library,
) error {
	raw, err := json.Marshal(struct {
		ClusterID string
		Libraries []Library
	}{
		clusterID,
		libraries,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/libraries/install",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.URL.Query().Add("cluster_id", clusterID)
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

// Uninstall sets libraries to be uninstalled on a cluster. The libraries
// aren’t uninstalled until the cluster is restarted. Uninstalling libraries
// that are not installed on the cluster has no impact but is not an error.
func (s *LibrariesService) Uninstall(
	ctx context.Context,
	clusterID string,
	libraries []Library,
) error {
	raw, err := json.Marshal(struct {
		ClusterID string
		Libraries []Library
	}{
		clusterID,
		libraries,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/libraries/uninstall",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.URL.Query().Add("cluster_id", clusterID)
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
