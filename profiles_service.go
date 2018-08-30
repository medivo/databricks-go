package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ProfilesService is a service for interacting with the DBFS.
type ProfilesService struct {
	client Client
}

// Add registers an instance profile in Databricks. In the UI, you can then
// give users the permission to use this instance profile when launching
// clusters. This API is only available to admin users.
func (s *ProfilesService) Add(
	ctx context.Context,
	profileARN string,
	skipValidation bool,
) error {
	raw, err := json.Marshal(struct {
		InstanceProfileARN string
		SkipValidation     bool
	}{
		profileARN,
		skipValidation,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/instance-profiles/add",
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

// List is used to return the instance profiles that the calling user can use
// to launch a cluster. This API is available to all users.
func (s *ProfilesService) List(
	ctx context.Context,
) ([]string, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/instance-profiles/get",
		nil,
	)
	if err != nil {
		return []string{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []string{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []string{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	listRes := struct {
		InstanceProfiles []string
	}{[]string{}}
	err = decoder.Decode(&listRes)

	return listRes.InstanceProfiles, err

}

// Remove is used to Remove the instance profile with the provided ARN.
// Existing clusters with this instance profile will continue to function.
//
// This API is only accessible to admin users.
func (s *ProfilesService) Remove(
	ctx context.Context,
	profileARN string,
) error {
	raw, err := json.Marshal(struct {
		InstanceProfileARN string
	}{
		profileARN,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/instance-profiles/remove",
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
