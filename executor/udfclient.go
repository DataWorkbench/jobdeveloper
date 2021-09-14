package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/gproto/pkg/udfpb"
)

type UdfClient struct {
	client udfpb.UdfmanagerClient
}

func NewUdfClient(conn *grpcwrap.ClientConn) (c UdfClient, err error) {
	c.client = udfpb.NewUdfmanagerClient(conn)
	return c, nil
}

func (s *UdfClient) DescribeUdfManager(ctx context.Context, ID string) (udfType string, name string, define string, err error) {
	var (
		req  request.DescribeUDF
		resp *response.DescribeUDF
	)

	req.UDFID = ID
	resp, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	udfType = resp.Info.GetUDFType()
	name = resp.Info.GetName()
	define = resp.Info.GetDefine()

	return
}
