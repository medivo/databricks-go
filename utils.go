package databricks

import (
	"fmt"
	"net/http"
)

type bearerRoundTripper struct {
	token string
}

// NewBearerHTTPClient uses a token as an authorization bearer.
// See:
// https://docs.databricks.com/api/latest/authentication.html#pass-token-to-bearer-authentication
func NewBearerHTTPClient(token string) *http.Client {
	client := *http.DefaultClient
	client.Transport = bearerRoundTripper{token: token}
	return &client
}

// RoundTrip implements the http.RoundTripper interface.
func (r bearerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.token))
	return http.DefaultClient.Do(req)
}
