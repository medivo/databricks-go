package databricks

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// nopCloser is a struct for converting a io.Reader to an io.ReadCloser.
type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return io.EOF }

// ErrExpected is an expected error.
var ErrExpected = fmt.Errorf("Expected")

// tripper is a http.RoundTripper that will return the error
// code it is configured with.
type tripper struct {
	code int
}

// RoundTrip implements the http.RoundTripper interface.
func (t tripper) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.code,
		Body: nopCloser{
			bytes.NewBuffer(
				[]byte("{'error':'expected'}"),
			),
		},
	}, nil
}

func injectedHTTPClient(res http.Response) *http.Client {
	return &http.Client{
		Transport: &injectedTripper{
			res: res,
		},
	}
}

// Non200HTTPClient is used for testing non 200 responses
// from a HTTP client.
var Non200HTTPClient = &http.Client{
	Transport: &tripper{418},
}

// injectedTripper is used to return expected HTTP responses.
type injectedTripper struct {
	res http.Response
}

// RoundTrip implements the http.RoundTripper interface.
func (t injectedTripper) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	return &t.res, nil
}

// errTripper is used to return expected HTTP transport
// errors.
type errTripper struct{}

// RoundTrip implements the http.RoundTripper interface.
func (t errTripper) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	return nil, ErrExpected
}

// BadTransportHTTPClient returns an ErrExpected during transport.
var BadTransportHTTPClient = &http.Client{
	Transport: errTripper{},
}

// ErrOpt is an option that returns an ErrExpected.
func ErrOpt(client *Client) error {
	return ErrExpected
}

func Test_Client(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account", ClientHTTPClient(Non200HTTPClient))
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}

	client, err = NewClient("test-account", ErrOpt)
	if err == nil {
		t.Fatalf("Failed to get expected error: ErrExpected")
	}
}

func Test_Client_Cluster(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	cluster := client.Cluster()
	if cluster == nil {
		t.Fatalf("Cluster returned nil")
	}
}

func Test_Client_DBFS(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	dbfs := client.DBFS()
	if dbfs == nil {
		t.Fatalf("DBFS returned nil")
	}
}

func Test_Client_Groups(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	groups := client.Groups()
	if groups == nil {
		t.Fatalf("Groups returned nil")
	}
}

func Test_Client_Jobs(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	jobs := client.Jobs()
	if jobs == nil {
		t.Fatalf("Jobs returned nil")
	}
}

func Test_Client_Libraries(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	libraries := client.Libraries()
	if libraries == nil {
		t.Fatalf("Libraries returned nil")
	}
}

func Test_Client_Profiles(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	profiles := client.Profiles()
	if profiles == nil {
		t.Fatalf("Profiles returned nil")
	}
}

func Test_Client_Secrets(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	secrets := client.Secrets()
	if secrets == nil {
		t.Fatalf("Secrets returned nil")
	}
}

func Test_Client_Token(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	token := client.Token()
	if token == nil {
		t.Fatalf("Token returned nil")
	}
}

func Test_Client_Workspace(t *testing.T) {
	t.Parallel()
	client, err := NewClient("test-account")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatalf("NewClient returned nil")
	}
	workspace := client.Workspace()
	if workspace == nil {
		t.Fatalf("Workspace returned nil")
	}
}
