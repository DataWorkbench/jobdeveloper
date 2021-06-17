package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/glog"
)

type JobdeveloperExecutor struct {
	logger           *glog.Logger
	sourceClient     SourceClient
	udfClient        UdfClient
	FlinkHome        string
	FlinkExecuteJars string
}

func NewJobDeveloperExecutor(l *glog.Logger, sClient SourceClient, uClient UdfClient, flinkHome string, flinkExecuteJars string) *JobdeveloperExecutor {
	ex := &JobdeveloperExecutor{
		logger:           l,
		sourceClient:     sClient,
		udfClient:        uClient,
		FlinkHome:        flinkHome,
		FlinkExecuteJars: flinkExecuteJars,
	}
	return ex
}

func (ex *JobdeveloperExecutor) JobParser(ctx context.Context, jobID string, workSpaceID string, engineID string, engineType string, command string, jobInfo string) (jobElementString string, err error) {
	if engineType == constants.ServerTypeFlink {
		var (
			dag        []constants.DagNode
			job        constants.FlinkNode
			jobElement constants.JobElementFlink
		)

		if err = json.Unmarshal([]byte(jobInfo), &job); err != nil {
			return
		}

		dag, err = turnToDagNode(job.Nodes)
		if err != nil {
			return
		}

		jobElement, err = printSqlAndElement(dag, job, ex.sourceClient, ex.udfClient, ex.FlinkHome, ex.FlinkExecuteJars, workSpaceID, engineID, jobID, command)
		if err != nil {
			return
		}
		jobElementByte, _ := json.Marshal(jobElement)
		jobElementString = string(jobElementByte)
	} else {
		err = fmt.Errorf("unknow server type " + engineType)
	}

	return
}

func (ex *JobdeveloperExecutor) JobFree(ctx context.Context, engineType string, jobResources string) (freeResources string, err error) {
	if engineType == constants.ServerTypeFlink {
		freeResources, err = FlinkJobFree(jobResources)
	}
	return
}

func (ex *JobdeveloperExecutor) FlinkNodeRelations(ctx context.Context) (relations string, err error) {
	_, relations, err = GetNodeRelation(constants.EmptyNode)
	return
}
