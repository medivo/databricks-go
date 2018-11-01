package databricks

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"

	"github.com/bgentry/go-netrc/netrc"
	"github.com/mitchellh/go-homedir"
)

type netrcRoundTripper struct{}

// RoundTrip implements the http.RoundTripper interface.
func (r netrcRoundTripper) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	if err := addAuthFromNetrc(req.URL); err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// NetrcHTTPClient adds auth from NETRC.
var NetrcHTTPClient = &http.Client{
	Transport: netrcRoundTripper{},
}

// NewBearerHTTPClient uses a token as an authorization bearer.
// See:
// https://docs.databricks.com/api/latest/authentication.html#pass-token-to-bearer-authentication
func NewBearerHTTPClient(token string) *http.Client {
	client := *http.DefaultClient
	client.Transport = bearerRoundTripper{token: token}
	return &client
}

type bearerRoundTripper struct {
	token string
}

// RoundTrip implements the http.RoundTripper interface.
func (r bearerRoundTripper) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.token))
	return http.DefaultClient.Do(req)
}

// addAuthFromNetrc adds auth information to the URL from the user's
// netrc file if it can be found. This will only add the auth info
// if the URL doesn't already have auth info specified and the
// the username is blank.
func addAuthFromNetrc(u *url.URL) error {
	// If the URL already has auth information, do nothing
	if u.User != nil && u.User.Username() != "" {
		return nil
	}

	// Get the netrc file path
	path := os.Getenv("NETRC")
	if path == "" {
		filename := ".netrc"
		if runtime.GOOS == "windows" {
			filename = "_netrc"
		}

		var err error
		path, err = homedir.Expand("~/" + filename)
		if err != nil {
			return err
		}
	}

	// If the file is not a file, then do nothing
	if fi, err := os.Stat(path); err != nil {
		// File doesn't exist, do nothing
		if os.IsNotExist(err) {
			return nil
		}

		// Some other error!
		return err
	} else if fi.IsDir() {
		// File is directory, ignore
		return nil
	}

	// Load up the netrc file
	net, err := netrc.ParseFile(path)
	if err != nil {
		return fmt.Errorf("Error parsing netrc file at %q: %s", path, err)
	}

	machine := net.FindMachine(u.Host)
	if machine == nil {
		// Machine not found, no problem
		return nil
	}

	// Set the user info
	u.User = url.UserPassword(machine.Login, machine.Password)
	return nil
}
