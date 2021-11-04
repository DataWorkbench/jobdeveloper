package executor

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/flinkpb"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"

	"github.com/DataWorkbench/glog"
)

type JobdeveloperExecutor struct {
	logger           *glog.Logger
	sourceClient     SourceClient
	udfClient        UdfClient
	engineClient     EngineClient
	fileClient       FileClient
	FlinkHome        string
	HadoopConf       string
	FlinkExecuteJars string
}

func NewJobDeveloperExecutor(l *glog.Logger, eClient EngineClient, sClient SourceClient, uClient UdfClient, fClient FileClient, flinkHome string, hadoopConf string, flinkExecuteJars string) *JobdeveloperExecutor {
	ex := &JobdeveloperExecutor{
		logger:           l,
		sourceClient:     sClient,
		udfClient:        uClient,
		engineClient:     eClient,
		fileClient:       fClient,
		FlinkHome:        flinkHome,
		HadoopConf:       hadoopConf,
		FlinkExecuteJars: flinkExecuteJars,
	}
	return ex
}

func (ex *JobdeveloperExecutor) JobParser(ctx context.Context, req *request.JobParser) (resp response.JobParser, err error) {
	var (
		jobElement response.JobParser
	)

	jobElement, err = parserJobInfo(ctx, req, ex.engineClient, ex.sourceClient, ex.udfClient, ex.fileClient, ex.FlinkHome, ex.HadoopConf, ex.FlinkExecuteJars)
	if err != nil {
		return
	}

	return jobElement, err
}

func (ex *JobdeveloperExecutor) JobFree(ctx context.Context, req *request.JobFree) (resp response.JobFree, err error) {
	resp, err = FlinkJobFree(req)
	return
}

func (ex *JobdeveloperExecutor) FlinkNodeRelations(ctx context.Context) (resp response.NodeRelations, err error) {
	_, resp.Relations, err = GetOperatorRelation(flinkpb.FlinkOperator_Empty)
	return
}
