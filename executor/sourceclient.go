package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/gproto/pkg/smpb"
)

type SourceClient struct {
	client smpb.SourcemanagerClient
}

func NewSourceClient(conn *grpcwrap.ClientConn) (c SourceClient, err error) {
	c.client = smpb.NewSourcemanagerClient(conn)
	return c, nil
}

func (s *SourceClient) DescribeSourceTable(ctx context.Context, ID string) (sourceID string, tableName string, url *model.TableUrl, err error) {
	var (
		req  request.DescribeTable
		resp *response.DescribeTable
	)

	req.TableID = ID
	resp, err = s.client.DescribeTable(ctx, &req)
	if err != nil {
		return
	}
	tableName = resp.Info.GetName()
	sourceID = resp.Info.GetSourceID()
	url = resp.Info.GetUrl()

	return
}

func (s *SourceClient) DescribeSourceManager(ctx context.Context, ID string) (sourceType string, url *model.SourceUrl, err error) {
	var (
		req  request.DescribeSource
		resp *response.DescribeSource
	)

	req.SourceID = ID
	resp, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	sourceType = resp.Info.GetSourceType()
	url = resp.Info.GetUrl()

	return
}
