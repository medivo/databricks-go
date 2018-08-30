package databricks

import (
	"bytes"
	"context"
	"net/http"
	"testing"
)

func badTransportGroupsHelper(t *testing.T) *GroupsService {
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
	groups := badTransportClient.Groups()
	if groups == nil {
		t.Fatalf("Groups returned nil")
	}
	return groups
}

func non200GroupsHelper(t *testing.T) *GroupsService {
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
	groups := non200Client.Groups()
	if groups == nil {
		t.Fatalf("Groups returned nil")
	}
	return groups
}

func successGroupsHelper(
	t *testing.T,
	res []byte,
	code int,
) *GroupsService {
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
	groups := successClient.Groups()
	if groups == nil {
		t.Fatalf("Groups returned nil")
	}

	return groups
}

func Test_GroupsService_AddMember(t *testing.T) {
	t.Parallel()
	res := []byte{}

	groups := successGroupsHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	err := groups.AddMember(ctx, "foo", "bar", "baz")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	err = groups.AddMember(ctx, "foo", "bar", "baz")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	err = groups.AddMember(ctx, "foo", "bar", "baz")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_GroupsService_Create(t *testing.T) {
	t.Parallel()
	res := []byte{}

	groups := successGroupsHelper(t, res, http.StatusCreated)

	ctx := context.Background()
	err := groups.Create(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	err = groups.Create(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	err = groups.Create(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_GroupsService_Members(t *testing.T) {
	t.Parallel()
	res := []byte(`{"members":[{"user_name":"foo"},{"group_name":"bar"}]}`)

	groups := successGroupsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	names, err := groups.Members(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) <= 1 {
		t.Fatal("Expected 2 members")
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	_, err = groups.Members(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	_, err = groups.Members(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_GroupsService_Groups(t *testing.T) {
	t.Parallel()
	res := []byte(`{"group_names":["foo","bar"]}`)

	groups := successGroupsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	names, err := groups.Groups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) <= 1 {
		t.Fatal("Expected 2 groups")
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	_, err = groups.Groups(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	_, err = groups.Groups(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_UserParentsService_UserParents(t *testing.T) {
	t.Parallel()
	res := []byte(`{"group_names":["foo","bar"]}`)

	groups := successGroupsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	names, err := groups.UserParents(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) <= 1 {
		t.Fatal("Expected 2 parents")
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	_, err = groups.UserParents(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	_, err = groups.UserParents(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_RemoveUserService_RemoveUser(t *testing.T) {
	t.Parallel()
	res := []byte{}

	groups := successGroupsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := groups.RemoveUser(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	err = groups.RemoveUser(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	err = groups.RemoveUser(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_DeleteService_Delete(t *testing.T) {
	t.Parallel()
	res := []byte{}

	groups := successGroupsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := groups.Delete(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	groups = non200GroupsHelper(t)

	err = groups.Delete(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	groups = badTransportGroupsHelper(t)

	err = groups.Delete(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
