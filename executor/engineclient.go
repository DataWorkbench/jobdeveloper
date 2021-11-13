package executor

import (
	"github.com/DataWorkbench/common/grpcwrap"
)

type EngineClient struct {
	//client enginepb.FlinkEngineServiceClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	//c.client = enginepb.NewFlinkEngineServiceClient(conn)
	return c, nil
}
