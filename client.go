package databricks

import (
	"fmt"
	"net/http"
)

// ListOrder is a listing order.
type ListOrder string

const (
	// Desc is descending.
	Desc ListOrder = "DESC"
	// Asc is ascending.
	Asc = "ASC"
)

// ClientOpt is used for configuring a Client.
type ClientOpt func(*Client) error

// ClientHTTPClient configures the Client's HTTP client.
func ClientHTTPClient(client *http.Client) ClientOpt {
	return func(c *Client) error {
		c.client = client
		return nil
	}
}

// Client is a Databricks client.
type Client struct {
	client  *http.Client
	account string
	url     string
}

// NewClient returns a new Databricks client.
func NewClient(account string, opts ...ClientOpt) (*Client, error) {
	c := &Client{
		account: account,
		url:     fmt.Sprintf("https://%s.cloud.databricks.com/api/", account),
		client:  http.DefaultClient,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Do is a proxy to the http.Client's Do method.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// Cluster returns a ClusterService for the corresponding client.
func (c *Client) Cluster() *ClusterService {
	return &ClusterService{
		client: *c,
	}
}

// DBFS returns a DBFSService for the corresponding client.
func (c *Client) DBFS() *DBFSService {
	return &DBFSService{
		client: *c,
	}
}

// Groups returns a GroupsService for the corresponding client.
func (c *Client) Groups() *GroupsService {
	return &GroupsService{
		client: *c,
	}
}

// Jobs returns a JobsService for the corresponding client.
func (c *Client) Jobs() *JobsService {
	return &JobsService{
		client: *c,
	}
}

// Libraries returns a LibrariesService for the corresponding client.
func (c *Client) Libraries() *LibrariesService {
	return &LibrariesService{
		client: *c,
	}
}

// Profiles returns a ProfilesService for the corresponding client.
func (c *Client) Profiles() *ProfilesService {
	return &ProfilesService{
		client: *c,
	}
}

// Secrets returns a SecretsService for the corresponding client.
func (c *Client) Secrets() *SecretsService {
	return &SecretsService{
		client: *c,
	}
}

// Token returns a TokenService for the corresponding client.
func (c *Client) Token() *TokenService {
	return &TokenService{
		client: *c,
	}
}

// Workspace returns a WorkspaceService for the corresponding client.
func (c *Client) Workspace() *WorkspaceService {
	return &WorkspaceService{
		client: *c,
	}
}
