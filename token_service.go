package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// TokenService is a service for interacting with the DBFS.
type TokenService struct {
	client Client
}

// Create and returns a token for a user. This call returns
// the error QUOTA_EXCEEDED if the user exceeds their token
// quota.
//
// This API is available to all users.
func (s *TokenService) Create(
	ctx context.Context,
	lifetimeSec uint,
) (string, *PublicTokenInfo, error) {
	raw, err := json.Marshal(struct {
		LifetimeSeconds uint
		Comment         string
	}{
		lifetimeSec,
		"",
	})
	if err != nil {
		return "", nil, err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/token/create",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return "", nil, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return "", nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return "", nil, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	createRes := struct {
		TokenValue string
		TokenInfo  *PublicTokenInfo
	}{}
	err = decoder.Decode(&createRes)

	return createRes.TokenValue, createRes.TokenInfo, nil
}

// List all the valid tokens for a user-workspace pair.
//
// This API is available to all users.
func (s *TokenService) List(
	ctx context.Context,
) ([]PublicTokenInfo, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/token/list",
		nil,
	)
	if err != nil {
		return []PublicTokenInfo{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []PublicTokenInfo{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []PublicTokenInfo{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	listRes := struct {
		TokenInfos []PublicTokenInfo `json:"token_infos"`
	}{[]PublicTokenInfo{}}
	err = decoder.Decode(&listRes)

	return listRes.TokenInfos, err
}

// Revoke revokes an access token. This call returns the error
// RESOURCE_DOES_NOT_EXIST if a token with the given ID is not valid.
//
// This API is available to all users.
func (s *TokenService) Revoke(
	ctx context.Context,
	tokens []PublicTokenInfo,
) error {
	raw, err := json.Marshal(struct {
		TokenInfos []PublicTokenInfo
	}{
		tokens,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/token/delete",
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
