package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
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
		req udfpb.DescribeRequest
		rep *udfpb.InfoReply
	)

	req.ID = ID
	rep, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	udfType = rep.GetUdfType()
	name = rep.GetName()
	define = rep.GetDefine()

	return
}
