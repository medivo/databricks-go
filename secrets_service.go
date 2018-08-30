package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// SecretsService is a service for interacting with the DBFS.
type SecretsService struct {
	client Client
}

// CreateSecretScope Creates a new secret scope.
//
// The scope name must consist of alphanumeric characters,
// dashes, underscores, and periods, and may not exceed 128
// characters. The maximum number of scopes in a workspace
// is 100.
func (s *SecretsService) CreateSecretScope(
	ctx context.Context,
	scope, initialManagePrincipal string,
) error {
	raw, err := json.Marshal(struct {
		Scope                  string
		InitialManagePrincipal string
	}{
		scope,
		initialManagePrincipal,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/scopes/create",
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

// DeleteSecretScope removes a secret scope.
func (s *SecretsService) DeleteSecretScope(
	ctx context.Context,
	scope string,
) error {
	raw, err := json.Marshal(struct {
		Scope string
	}{
		scope,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/scopes/delete",
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

// ListSecretScopes returns all secret scopes available in the workspace.
func (s *SecretsService) ListSecretScopes(
	ctx context.Context,
) ([]SecretScope, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/secrets/scopes/list",
		nil,
	)
	if err != nil {
		return []SecretScope{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []SecretScope{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []SecretScope{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}

	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	listRes := struct {
		Scopes []SecretScope
	}{[]SecretScope{}}
	err = decoder.Decode(&listRes)

	return listRes.Scopes, err
}

// PutSecret inserts a secret under the provided scope with the given name. If
// a secret already exists with the same name, this command overwrites the
// existing secret’s value. The server encrypts the secret using the secret
// scope’s encryption settings before storing it. You must have WRITE or MANAGE
// permission on the secret scope.
//
// The secret key must consist of alphanumeric characters, dashes, underscores,
// and periods, and cannot exceed 128 characters. The maximum allowed secret
// value size is 128 KB. The maximum number of secrets in a given scope is
// 1000.
func (s *SecretsService) PutSecret(
	ctx context.Context,
	scope, key, value string,
) error {
	raw, err := json.Marshal(struct {
		Scope       string
		Key         string
		StringValue string
	}{
		scope,
		key,
		value,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/put",
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

// DeleteSecret removes a secret.
func (s *SecretsService) DeleteSecret(
	ctx context.Context,
	scope, key string,
) error {
	raw, err := json.Marshal(struct {
		Scope string
		Key   string
	}{
		scope,
		key,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/delete",
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

// ListSecrets returns the secret keys that are stored at this scope. This is a
// metadata-only operation; secret data cannot be retrieved using this API.
// Users need READ permission to make this call.
func (s *SecretsService) ListSecrets(
	ctx context.Context,
) ([]SecretMetadata, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/secrets/list",
		nil,
	)
	if err != nil {
		return []SecretMetadata{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []SecretMetadata{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []SecretMetadata{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}

	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	listRes := struct {
		Secrets []SecretMetadata
	}{[]SecretMetadata{}}
	err = decoder.Decode(&listRes)

	return listRes.Secrets, err
}

// PutSecretACL Creates or overwrites the ACL associated with the given
// principal (user or group) on the specified scope point. In general, a user
// or group will use the most powerful permission available to them, and
// permissions are ordered as follows:
//
// MANAGE - Allowed to change ACLs, and read and write to this secret scope.
// WRITE - Allowed to read and write to this secret scope.  READ - Allowed to
// read this secret scope and list what secrets are available.  Note that in
// general, secret values can only be read from within a command on a cluster
// (for example, through a notebook). There is no API to read the actual secret
// value material outside of a cluster. However, the user’s permission will be
// applied based on who is executing the command, and they must have at least
// READ permission.
//
// Users must have the MANAGE permission to invoke this API.
func (s *SecretsService) PutSecretACL(
	ctx context.Context,
	scope, principal string,
	permission string,
) error {
	raw, err := json.Marshal(struct {
		Scope      string
		Principal  string
		Permission string
	}{
		scope,
		principal,
		permission,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/acls/put",
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

// DeleteSecretACL removes a Secret ACL.
func (s *SecretsService) DeleteSecretACL(
	ctx context.Context,
	scope, principal string,
) error {
	raw, err := json.Marshal(struct {
		Scope     string
		Principal string
	}{
		scope,
		principal,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/secrets/acls/delete",
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

// GetSecretACL Describes the details about the given ACL, such as the group
// and permission.
//
// Users must have the MANAGE permission to invoke this API.
func (s *SecretsService) GetSecretACL(
	ctx context.Context,
	scope, principal string,
) (string, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/secrets/acls/get",
		nil,
	)
	if err != nil {
		return "", err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("scope", scope)
	q.Add("principal", principal)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return "", err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return "", fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}

	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	getRes := struct {
		Principal  string
		Permission string
	}{}
	err = decoder.Decode(&getRes)

	return getRes.Permission, err
}

// ListSecretACLs Lists the ACLs set on the given scope.
//
// Users must have the MANAGE permission to invoke this API.
func (s *SecretsService) ListSecretACLs(
	ctx context.Context,
	scope string,
) ([]ACLItem, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/secrets/acls/list",
		nil,
	)
	if err != nil {
		return []ACLItem{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("scope", scope)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return []ACLItem{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []ACLItem{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}

	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	listRes := struct {
		Items []ACLItem
	}{[]ACLItem{}}
	err = decoder.Decode(&listRes)

	return listRes.Items, err
}
