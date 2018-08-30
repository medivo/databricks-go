package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportSecretsHelper(t *testing.T) *SecretsService {
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
	secrets := badTransportClient.Secrets()
	if secrets == nil {
		t.Fatalf("Secrets returned nil")
	}
	return secrets
}

func non200SecretsHelper(t *testing.T) *SecretsService {
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
	secrets := non200Client.Secrets()
	if secrets == nil {
		t.Fatalf("Secrets returned nil")
	}
	return secrets
}

func successSecretsHelper(
	t *testing.T,
	res []byte,
	code int,
) *SecretsService {
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
	secrets := successClient.Secrets()
	if secrets == nil {
		t.Fatalf("Secrets returned nil")
	}

	return secrets
}

func Test_SecretsService_CreateSecretScope(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.CreateSecretScope(ctx, "admin", "user-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.CreateSecretScope(ctx, "admin", "user-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.CreateSecretScope(ctx, "admin", "user-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_DeleteSecretScope(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.DeleteSecretScope(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.DeleteSecretScope(ctx, "admin")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.DeleteSecretScope(ctx, "admin")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
func Test_SecretsService_ListSecretScopes(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Scopes []SecretScope
	}{
		[]SecretScope{
			SecretScope{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	secrets := successSecretsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	scopes, err := secrets.ListSecretScopes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) == 0 {
		t.Fatalf("Expected more than 0 scopes")
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	_, err = secrets.ListSecretScopes(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	_, err = secrets.ListSecretScopes(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_PutSecret(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.PutSecret(ctx, "admin", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.PutSecret(ctx, "admin", "foo", "bar")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.PutSecret(ctx, "admin", "foo", "bar")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_DeleteSecret(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.DeleteSecret(ctx, "admin", "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.DeleteSecret(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.DeleteSecret(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_ListSecrets(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Secrets []SecretMetadata
	}{
		[]SecretMetadata{
			SecretMetadata{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	secrets := successSecretsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	scopes, err := secrets.ListSecrets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) == 0 {
		t.Fatalf("Expected more than 0 scopes")
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	_, err = secrets.ListSecrets(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	_, err = secrets.ListSecrets(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_PutSecretACL(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.PutSecretACL(ctx, "admin", "foo", "MANAGE")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.PutSecretACL(ctx, "admin", "foo", "MANAGE")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.PutSecretACL(ctx, "admin", "foo", "MANAGE")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_DeleteSecretACL(t *testing.T) {
	t.Parallel()
	secrets := successSecretsHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := secrets.DeleteSecretACL(ctx, "admin", "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	err = secrets.DeleteSecretACL(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	err = secrets.DeleteSecretACL(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_GetSecretACL(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Principal  string
		Permission string
	}{
		"foo",
		"bar",
	})
	if err != nil {
		t.Fatal(err)
	}
	secrets := successSecretsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	acl, err := secrets.GetSecretACL(ctx, "admin", "foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(acl) == 0 {
		t.Fatalf("Empty ACL")
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	_, err = secrets.GetSecretACL(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	_, err = secrets.GetSecretACL(ctx, "admin", "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_SecretsService_ListSecretACLs(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Items []ACLItem
	}{
		[]ACLItem{
			ACLItem{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	secrets := successSecretsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	acls, err := secrets.ListSecretACLs(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if len(acls) == 0 {
		t.Fatalf("Empty ACL")
	}

	// Non 200 test
	secrets = non200SecretsHelper(t)

	_, err = secrets.ListSecretACLs(ctx, "admin")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	secrets = badTransportSecretsHelper(t)

	_, err = secrets.ListSecretACLs(ctx, "admin")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
