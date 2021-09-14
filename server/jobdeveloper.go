package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"

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

func (s *JobDeveloperServer) JobParser(ctx context.Context, req *request.JobParser) (*response.JobParser, error) {
	resp, err := s.executor.JobParser(ctx, req)
	return &resp, err
}

func (s *JobDeveloperServer) NodeRelations(ctx context.Context, req *model.EmptyStruct) (*response.NodeRelations, error) {
	resp, err := s.executor.FlinkNodeRelations(ctx)
	return &resp, err
}

func (s *JobDeveloperServer) JobFree(ctx context.Context, req *request.JobFree) (*response.JobFree, error) {
	resp, err := s.executor.JobFree(ctx, req)
	return &resp, err
}
