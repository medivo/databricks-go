package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportTokenHelper(t *testing.T) *TokenService {
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
	token := badTransportClient.Token()
	if token == nil {
		t.Fatalf("Token returned nil")
	}
	return token
}

func non200TokenHelper(t *testing.T) *TokenService {
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
	token := non200Client.Token()
	if token == nil {
		t.Fatalf("Token returned nil")
	}
	return token
}

func successTokenHelper(
	t *testing.T,
	res []byte,
	code int,
) *TokenService {
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
	token := successClient.Token()
	if token == nil {
		t.Fatalf("Token returned nil")
	}

	return token
}

func Test_TokenService_Create(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		TokenValue string
		TokenInfo  *PublicTokenInfo
	}{
		"token-123",
		&PublicTokenInfo{},
	})
	if err != nil {
		t.Fatal(err)
	}
	token := successTokenHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	toke, tokeInfo, err := token.Create(ctx, uint(32))
	if err != nil {
		t.Fatal(err)
	}
	if len(toke) == 0 {
		t.Fatalf("Expected token to not be empty")
	}
	if tokeInfo == nil {
		t.Fatalf("Expected token info to not be nil")
	}

	// Non 200 test
	token = non200TokenHelper(t)

	_, _, err = token.Create(ctx, uint(32))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	token = badTransportTokenHelper(t)

	_, _, err = token.Create(ctx, uint(32))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_TokenService_List(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		TokenInfos []PublicTokenInfo `json:"token_infos"`
	}{
		[]PublicTokenInfo{
			PublicTokenInfo{
				TokenID: "token-123",
				Comment: "foo",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	token := successTokenHelper(t, res, http.StatusOK)

	ctx := context.Background()
	tokens, err := token.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) == 0 {
		t.Fatalf("Expected token to not be empty")
	}

	// Non 200 test
	token = non200TokenHelper(t)

	_, err = token.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	token = badTransportTokenHelper(t)

	_, err = token.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
func Test_TokenService_Revoke(t *testing.T) {
	t.Parallel()
	tokens := []PublicTokenInfo{
		PublicTokenInfo{},
	}
	token := successTokenHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := token.Revoke(ctx, tokens)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	token = non200TokenHelper(t)

	err = token.Revoke(ctx, tokens)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	token = badTransportTokenHelper(t)

	err = token.Revoke(ctx, tokens)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
