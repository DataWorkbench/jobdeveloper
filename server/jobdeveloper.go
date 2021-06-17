package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/DataWorkbench/jobdeveloper/executor"
)

type JobDeveloperServer struct {
	jobdevpb.UnimplementedJobdeveloperServer
	executor   *executor.JobdeveloperExecutor
	emptyReply *model.EmptyStruct
}

func NewJobDeveloperServer(executor *executor.JobdeveloperExecutor) *JobDeveloperServer {
	return &JobDeveloperServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

func (s *JobDeveloperServer) JobParser(ctx context.Context, req *jobdevpb.JobParserRequest) (r *jobdevpb.JobElement, err error) {
	var resp jobdevpb.JobElement
	resp.JobElement, err = s.executor.JobParser(ctx, req.GetID(), req.GetWorkspaceID(), req.GetEngineID(), req.GetEngineType(), req.GetCommand(), req.GetJobInfo())
	return &resp, err
}

func (s *JobDeveloperServer) NodeRelations(ctx context.Context, req *model.EmptyStruct) (*jobdevpb.Relations, error) {
	var relations jobdevpb.Relations
	r, err := s.executor.FlinkNodeRelations(ctx)
	relations.Relations = r
	return &relations, err
}

func (s *JobDeveloperServer) JobFree(ctx context.Context, req *jobdevpb.JobFreeRequest) (r *jobdevpb.JobFreeAction, err error) {
	var resp jobdevpb.JobFreeAction
	resp.JobResources, err = s.executor.JobFree(ctx, req.GetEngineType(), req.GetJobResources())
	return &resp, err
}
