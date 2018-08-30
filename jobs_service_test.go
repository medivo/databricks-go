package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func badTransportJobsHelper(t *testing.T) *JobsService {
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
	jobs := badTransportClient.Jobs()
	if jobs == nil {
		t.Fatalf("Jobs returned nil")
	}
	return jobs
}

func non200JobsHelper(t *testing.T) *JobsService {
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
	jobs := non200Client.Jobs()
	if jobs == nil {
		t.Fatalf("Jobs returned nil")
	}
	return jobs
}

func successJobsHelper(
	t *testing.T,
	res []byte,
	code int,
) *JobsService {
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
	jobs := successClient.Jobs()
	if jobs == nil {
		t.Fatalf("Jobs returned nil")
	}

	return jobs
}

func Test_JobsService_Create(t *testing.T) {
	t.Parallel()
	res := []byte(`{"job_id":123}`)

	jobs := successJobsHelper(t, res, http.StatusCreated)

	createReq := &JobCreateRequest{}
	ctx := context.Background()
	id, err := jobs.Create(ctx, createReq)
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("Expected valid job id")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.Create(ctx, createReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.Create(ctx, createReq)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_List(t *testing.T) {
	t.Parallel()
	res := []byte(`{"jobs":[{"job_id":1}]}`)

	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	jobList, err := jobs.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobList) == 0 {
		t.Fatalf("Expected more than 0 jobs")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.List(ctx)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_Delete(t *testing.T) {
	t.Parallel()
	res := []byte{}

	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := jobs.Delete(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	err = jobs.Delete(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	err = jobs.Delete(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_Get(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(&JobGetResponse{})
	if err != nil {
		t.Fatal(err)
	}

	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	jobRes, err := jobs.Get(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}
	if jobRes == nil {
		t.Fatalf("Expected JobGetResponse")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.Get(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.Get(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_Reset(t *testing.T) {
	t.Parallel()
	res := []byte{}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	err := jobs.Reset(ctx, int64(123), JobSettings{})
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	err = jobs.Reset(ctx, int64(123), JobSettings{})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	err = jobs.Reset(ctx, int64(123), JobSettings{})
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunNow(t *testing.T) {
	t.Parallel()
	res := []byte(`{"run_id":123, "number_in_job":456}`)
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	settings := &JobRunNowSettings{}

	runID, runNum, err := jobs.RunNow(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	if runID <= 0 {
		t.Fatalf("Expected run ID to be > 0")
	}
	if runNum <= 0 {
		t.Fatalf("Expected run number to be > 0")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, _, err = jobs.RunNow(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, _, err = jobs.RunNow(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunSubmit(t *testing.T) {
	t.Parallel()
	res := []byte(`{"run_id":123}`)
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	settings := &JobSubmitSettings{}

	runID, err := jobs.RunSubmit(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	if runID <= 0 {
		t.Fatalf("Expected run ID to be > 0")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.RunSubmit(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.RunSubmit(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsList(t *testing.T) {
	t.Parallel()
	res := []byte(`{"has_more":true,"runs":[{}]}`)
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()
	settings := &JobRunListRequest{}

	runs, more, err := jobs.RunsList(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	if !more {
		t.Fatalf("Expected more :)")
	}
	if len(runs) == 0 {
		t.Fatalf("Expected more runs")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, _, err = jobs.RunsList(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, _, err = jobs.RunsList(ctx, settings)
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsGet(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(&JobRunGetResponse{})
	if err != nil {
		t.Fatal(err)
	}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()

	getRes, err := jobs.RunsGet(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}
	if getRes == nil {
		t.Fatalf("Expected more :)")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.RunsGet(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.RunsGet(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsExport(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		Views []View
	}{
		[]View{
			View{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()

	views, err := jobs.RunsExport(ctx, int64(123), "ALL")
	if err != nil {
		t.Fatal(err)
	}
	if len(views) == 0 {
		t.Fatalf("Expected more :)")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, err = jobs.RunsExport(ctx, int64(123), "ALL")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, err = jobs.RunsExport(ctx, int64(123), "ALL")
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsCancel(t *testing.T) {
	t.Parallel()
	res := []byte{}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()

	err := jobs.RunsCancel(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	err = jobs.RunsCancel(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	err = jobs.RunsCancel(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsGetOutput(t *testing.T) {
	t.Parallel()
	run := &Run{}
	type nbOutput struct {
		Result string
	}
	res, err := json.Marshal(struct {
		NotebookOutput *nbOutput
		Error          *string
		Metadata       *Run
	}{&nbOutput{"output"}, nil, run})
	if err != nil {
		t.Fatal(err)
	}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()

	output, run, err := jobs.RunsGetOutput(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}
	if len(output) == 0 {
		t.Fatalf("Expected output to not be empty")
	}
	if run == nil {
		t.Fatalf("Expected run to not be nil")
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	_, _, err = jobs.RunsGetOutput(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	_, _, err = jobs.RunsGetOutput(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}

func Test_JobsService_RunsDelete(t *testing.T) {
	t.Parallel()
	res, err := json.Marshal(struct {
		NotebookOutput *struct {
			Result string
		}
		Error    *string
		Metadata *Run
	}{})
	if err != nil {
		t.Fatal(err)
	}
	jobs := successJobsHelper(t, res, http.StatusOK)

	ctx := context.Background()

	err = jobs.RunsDelete(ctx, int64(123))
	if err != nil {
		t.Fatal(err)
	}

	// Non 200 test
	jobs = non200JobsHelper(t)

	err = jobs.RunsDelete(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}

	// Transport error test
	jobs = badTransportJobsHelper(t)

	err = jobs.RunsDelete(ctx, int64(123))
	if err == nil {
		t.Fatalf("Expected error to not be nil")
	}
}
