package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GroupsService is a service for interacting with the DBFS.
type GroupsService struct {
	client Client
}

// AddMember adds a user or group to a group. This call returns an error
// RESOURCE_DOES_NOT_EXIST if a user or group with the given name does not
// exist, or if a group with the given parent name does not exist.
func (s *GroupsService) AddMember(
	ctx context.Context,
	userName, groupName, parentName string,
) error {
	raw, err := json.Marshal(struct {
		UserName   string `json:"user_name,omitempty"`
		GroupName  string `json:"group_name,omitempty"`
		ParentName string `json:"parent_name"`
	}{
		userName,
		groupName,
		parentName,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/groups/add-member",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil

}

// Create a new group with the given name. This call returns an error
// RESOURCE_ALREADY_EXISTS if a group with the given name already exists.
func (s *GroupsService) Create(
	ctx context.Context,
	groupName string,
) error {
	raw, err := json.Marshal(struct {
		GroupName string `json:"group_name"`
	}{
		groupName,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/groups/create",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Members returns all of the members of a particular group. This call returns
// an error RESOURCE_DOES_NOT_EXIST if a group with the given name does not
// exist.
func (s *GroupsService) Members(
	ctx context.Context,
	groupName string,
) ([]PrincipalName, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/groups/list-members",
		nil,
	)
	if err != nil {
		return []PrincipalName{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("group_name", groupName)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return []PrincipalName{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []PrincipalName{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	membersRes := struct {
		Members []PrincipalName
	}{[]PrincipalName{}}
	err = decoder.Decode(&membersRes)

	return membersRes.Members, err
}

// Groups returns all of the groups in an organization.
func (s *GroupsService) Groups(
	ctx context.Context,
) ([]string, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/groups/list",
		nil,
	)
	if err != nil {
		return []string{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []string{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []string{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	groupRes := struct {
		GroupNames []string `json:"group_names"`
	}{[]string{}}
	err = decoder.Decode(&groupRes)

	return groupRes.GroupNames, err
}

// UserParents returns all of the Parent groups of a user.
func (s *GroupsService) UserParents(
	ctx context.Context,
	userName string,
) ([]string, error) {
	return s.parents(ctx, "", userName)
}

// GroupParents returns all of the Parent groups of a group.
func (s *GroupsService) GroupParents(
	ctx context.Context,
	groupName string,
) ([]string, error) {
	return s.parents(ctx, groupName, "")
}

func (s *GroupsService) parents(
	ctx context.Context,
	groupName, userName string,
) ([]string, error) {
	if len(groupName) > 0 && len(userName) > 0 {
		return []string{}, fmt.Errorf(
			"Must specify either group_name OR user_name")
	}
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/groups/list-parents",
		nil,
	)
	if err != nil {
		return []string{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	if len(groupName) > 0 {
		q.Add("group_name", groupName)
	}
	if len(userName) > 0 {
		q.Add("user_name", userName)
	}
	req.URL.RawQuery = q.Encode()

	res, err := s.client.client.Do(req)
	if err != nil {
		return []string{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []string{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	groupRes := struct {
		GroupNames []string `json:"group_names"`
	}{[]string{}}
	err = decoder.Decode(&groupRes)

	return groupRes.GroupNames, err
}

// RemoveUser removes a User from a group.
func (s *GroupsService) RemoveUser(
	ctx context.Context,
	userName string,
) error {
	raw, err := json.Marshal(struct {
		UserName string `json:"user_name"`
	}{
		userName,
	})
	if err != nil {
		return err
	}
	return s.remove(ctx, raw)
}

// RemoveGroup removes a Group from a group.
func (s *GroupsService) RemoveGroup(
	ctx context.Context,
	groupName string,
) error {
	raw, err := json.Marshal(struct {
		GroupName string `json:"group_name"`
	}{
		groupName,
	})
	if err != nil {
		return err
	}
	return s.remove(ctx, raw)
}

func (s *GroupsService) remove(
	ctx context.Context,
	raw []byte,
) error {
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/groups/remove-member",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}

// Delete is used to delete a group.
func (s *GroupsService) Delete(
	ctx context.Context,
	groupName string,
) error {
	raw, err := json.Marshal(struct {
		GroupName string `json:"group_name"`
	}{
		groupName,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/groups/delete",
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
			"Failed to returns 2XX response: %d", res.StatusCode)
	}

	return nil
}
