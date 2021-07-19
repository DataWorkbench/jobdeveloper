package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/smpb"
)

type SourceClient struct {
	client smpb.SourcemanagerClient
}

func NewSourceClient(conn *grpcwrap.ClientConn) (c SourceClient, err error) {
	c.client = smpb.NewSourcemanagerClient(conn)
	return c, nil
}

func (s *SourceClient) DescribeSourceTable(ctx context.Context, ID string) (sourceID string, tableName string, url string, err error) {
	var (
		req smpb.SotDescribeRequest
		rep *smpb.SotInfoReply
	)

	req.ID = ID
	rep, err = s.client.SotDescribe(ctx, &req)
	if err != nil {
		return
	}
	tableName = rep.GetName()
	sourceID = rep.GetSourceID()
	url = rep.GetUrl()

	return
}

func (s *SourceClient) DescribeSourceManager(ctx context.Context, ID string) (sourceType string, url string, err error) {
	var (
		req smpb.DescribeRequest
		rep *smpb.InfoReply
	)

	req.ID = ID
	rep, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	sourceType = rep.GetSourceType()
	url = rep.GetUrl()

	return
}
