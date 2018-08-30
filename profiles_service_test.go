package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportProfilesHelper(t *testing.T) *ProfilesService {
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
	profiles := badTransportClient.Profiles()
	if profiles == nil {
		t.Fatalf("Profiles returned nil")
	}
	return profiles
}

func non200ProfilesHelper(t *testing.T) *ProfilesService {
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
	profiles := non200Client.Profiles()
	if profiles == nil {
		t.Fatalf("Profiles returned nil")
	}
	return profiles
}

func successProfilesHelper(
	t *testing.T,
	res []byte,
	code int,
) *ProfilesService {
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
	profiles := successClient.Profiles()
	if profiles == nil {
		t.Fatalf("Profiles returned nil")
	}

	return profiles
}

func Test_ProfilesService_Add(t *testing.T) {
	t.Parallel()
	profiles := successProfilesHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := profiles.Add(ctx, "profile-123", false)
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	profiles = non200ProfilesHelper(t)

	err = profiles.Add(ctx, "profile-123", false)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	profiles = badTransportProfilesHelper(t)

	err = profiles.Add(ctx, "profile-123", false)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ProfilesService_List(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		InstanceProfiles []string
	}{
		[]string{
			"foo",
			"bar",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	profiles := successProfilesHelper(t, res, http.StatusOK)

	ctx := context.Background()
	profs, err := profiles.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(profs) == 0 {
		t.Fatalf("Expected more than 0 profiles")
	}

	// Non 200 test
	profiles = non200ProfilesHelper(t)

	_, err = profiles.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	profiles = badTransportProfilesHelper(t)

	_, err = profiles.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_ProfilesService_Remove(t *testing.T) {
	t.Parallel()
	profiles := successProfilesHelper(t, []byte{}, http.StatusOK)

	ctx := context.Background()
	err := profiles.Remove(ctx, "profile-123")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	profiles = non200ProfilesHelper(t)

	err = profiles.Remove(ctx, "profile-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	profiles = badTransportProfilesHelper(t)

	err = profiles.Remove(ctx, "profile-123")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
