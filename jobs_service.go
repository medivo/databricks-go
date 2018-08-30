package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// JobsService is a service for interacting with the DBFS.
type JobsService struct {
	client Client
}

// Create is used to create a new job with the provided settings.
func (s *JobsService) Create(
	ctx context.Context,
	createReq *JobCreateRequest,
) (int64, error) {
	raw, err := json.Marshal(createReq)
	if err != nil {
		return int64(-1), err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/create",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return int64(-1), err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return int64(-1), err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return int64(-1), fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	createRes := struct {
		JobID int64 `json:"job_id"`
	}{}
	err = decoder.Decode(&createRes)

	return createRes.JobID, err
}

// List returns all jobs.
func (s *JobsService) List(
	ctx context.Context,
) ([]Job, error) {
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/list",
		nil,
	)
	if err != nil {
		return []Job{}, err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return []Job{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []Job{}, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	listRes := struct {
		Jobs []Job
	}{}
	err = decoder.Decode(&listRes)

	return listRes.Jobs, err
}

// Delete removes a job.
func (s *JobsService) Delete(
	ctx context.Context,
	jobID int64,
) error {
	raw, err := json.Marshal(struct {
		JobID int64
	}{
		jobID,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/delete",
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

// Get returns a job info.
func (s *JobsService) Get(
	ctx context.Context,
	jobID int64,
) (*JobGetResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/jobs/get",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("job_id", fmt.Sprintf("%d", jobID))
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return nil, fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	var jobGetRes JobGetResponse
	err = decoder.Decode(&jobGetRes)

	return &jobGetRes, err
}

// Reset is used to overwrite the settings of the job with the provided
// settings.
func (s *JobsService) Reset(
	ctx context.Context,
	jobID int64,
	settings JobSettings,
) error {
	raw, err := json.Marshal(struct {
		JobID       int64
		NewSettings JobSettings
	}{
		jobID,
		settings,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/reset",
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

// RunNow runs the job now, and returns the run_id of the
// triggered run. It returns the Run ID and the number in
// the job as well as any errors.
func (s *JobsService) RunNow(
	ctx context.Context,
	settings *JobRunNowSettings,
) (int64, int64, error) {
	raw, err := json.Marshal(settings)
	if err != nil {
		return int64(-1), int64(-1), err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/run-now",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return int64(-1), int64(-1), err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return int64(-1), int64(-1), err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return int64(-1), int64(-1), fmt.Errorf(
			"Failed to returns 2XX response: %d", res.StatusCode)
	}
	defer req.Body.Close()
	decoder := json.NewDecoder(res.Body)

	runRes := struct {
		RunID       int64 `json:"run_id"`
		NumberInJob int64 `json:"number_in_job"`
	}{}
	err = decoder.Decode(&runRes)

	return runRes.RunID, runRes.NumberInJob, err
}

// RunSubmit submits a one-time run with the provided
// settings. This endpoint doesn’t require a Databricks job
// to be created. You can directly submit your workload.
// Runs submitted via this endpoint don’t show up in the
// UI. Once the run is submitted, you can use the
// jobs/runs/get API to check the run state.
func (s *JobsService) RunSubmit(
	ctx context.Context,
	settings *JobSubmitSettings,
) (int64, error) {
	raw, err := json.Marshal(settings)
	if err != nil {
		return int64(-1), err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/runs/submit",
		bytes.NewBuffer(raw),
	)
	if err != nil {
		return int64(-1), err
	}
	req = req.WithContext(ctx)
	res, err := s.client.client.Do(req)
	if err != nil {
		return int64(-1), err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return int64(-1), fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer req.Body.Close()
	decoder := json.NewDecoder(res.Body)

	runRes := struct {
		RunID int64 `json:"run_id"`
	}{}
	err = decoder.Decode(&runRes)

	return runRes.RunID, err
}

// RunsList returns runs from most recently started to least. It returns a
// []Run and whether or not there are more available for paging.
func (s *JobsService) RunsList(
	ctx context.Context,
	runListReq *JobRunListRequest,
) ([]Run, bool, error) {
	if runListReq.ActiveOnly != nil && runListReq.CompleteOnly != nil {
		return []Run{}, false, fmt.Errorf(
			"Can only request active only OR complete only")
	}

	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/jobs/runs/list",
		nil,
	)
	if err != nil {
		return []Run{}, false, err
	}
	req = req.WithContext(ctx)
	req.URL.Query().Add("job_id", fmt.Sprintf("%d", runListReq.JobID))
	req.URL.Query().Add("offset", fmt.Sprintf("%d", runListReq.Offset))
	req.URL.Query().Add("limit", fmt.Sprintf("%d", runListReq.Limit))
	if runListReq.ActiveOnly != nil {
		req.URL.Query().Add(
			"active_only", fmt.Sprintf("%#v", runListReq.ActiveOnly))
	} else {
		req.URL.Query().Add(
			"complete_only", fmt.Sprintf("%#v", runListReq.CompleteOnly))
	}
	res, err := s.client.client.Do(req)
	if err != nil {
		return []Run{}, false, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []Run{}, false, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	listRes := struct {
		Runs    []Run `json:"runs"`
		HasMore bool  `json:"has_more"`
	}{[]Run{}, false}
	err = decoder.Decode(&listRes)

	return listRes.Runs, listRes.HasMore, err
}

// RunsGet retrieves the metadata of a run.
func (s *JobsService) RunsGet(
	ctx context.Context,
	runID int64,
) (*JobRunGetResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/jobs/runs/get",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("run_id", fmt.Sprintf("%d", runID))
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return nil, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	var jobGetRes JobRunGetResponse
	err = decoder.Decode(&jobGetRes)

	return &jobGetRes, err
}

// RunsExport exports and retrieves the job run task.
func (s *JobsService) RunsExport(
	ctx context.Context,
	runID int64,
	viewToExport string,
) ([]View, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/jobs/runs/export",
		nil,
	)
	if err != nil {
		return []View{}, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("run_id", fmt.Sprintf("%d", runID))
	q.Add("views_to_export", viewToExport)
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return []View{}, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return []View{}, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	exportRes := struct {
		Views []View
	}{[]View{}}
	err = decoder.Decode(&exportRes)

	return exportRes.Views, err
}

// RunsCancel cancels a run. The run is canceled asynchronously, so when this
// request completes, the run may still be running. The run will be terminated
// shortly. If the run is already in a terminal life_cycle_state, this method
// is a no-op.
func (s *JobsService) RunsCancel(
	ctx context.Context,
	runID int64,
) error {
	raw, err := json.Marshal(struct {
		RunID int64
	}{
		runID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/runs/cancel",
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

// RunsGetOutput retrieve the output of a run. When a notebook task returns
// value through the Notebook Workflow Exit call, you can use this endpoint to
// retrieve that value. Databricks restricts this API to return the first 5 MB
// of the output. For returning a larger result, you can store job results in a
// cloud storage service.
//
// Runs are automatically removed after 60 days. If you to want to reference
// them beyond 60 days, you should save old run results before they expire. To
// export using the UI, see Export job run results. To export using the Job
// API, see Runs Export.
func (s *JobsService) RunsGetOutput(
	ctx context.Context,
	runID int64,
) (string, *Run, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		s.client.url+"2.0/jobs/runs/get-output",
		nil,
	)
	if err != nil {
		return "", nil, err
	}
	req = req.WithContext(ctx)
	q := req.URL.Query()
	q.Add("run_id", fmt.Sprintf("%d", runID))
	req.URL.RawQuery = q.Encode()
	res, err := s.client.client.Do(req)
	if err != nil {
		return "", nil, err
	}
	if res.StatusCode >= 300 || res.StatusCode <= 199 {
		return "", nil, fmt.Errorf(
			"Failed to return a 2XX response: %d", res.StatusCode)
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)

	outputRes := struct {
		NotebookOutput *struct {
			Result string
		}
		Error    *string
		Metadata *Run
	}{}
	err = decoder.Decode(&outputRes)
	if err != nil {
		return "", nil, err
	}
	if outputRes.Error != nil {
		return "", nil, fmt.Errorf(*outputRes.Error)
	}
	if outputRes.NotebookOutput == nil {
		return "", nil, fmt.Errorf("No output received")
	}

	return outputRes.NotebookOutput.Result, outputRes.Metadata, nil
}

// RunsDelete deletes a non-active run. Returns an error if the run is active.
func (s *JobsService) RunsDelete(
	ctx context.Context,
	runID int64,
) error {
	raw, err := json.Marshal(struct {
		RunID int64
	}{
		runID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		s.client.url+"2.0/jobs/runs/delete",
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
