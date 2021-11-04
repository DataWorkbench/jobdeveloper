package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/flinkpb"
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

func (s *SourceClient) DescribeSourceTable(ctx context.Context, ID string) (sourceID string, tableName string, tableSchema *flinkpb.TableSchema, err error) {
	var (
		req  request.DescribeTable
		resp *response.DescribeTable
	)

	req.TableId = ID
	resp, err = s.client.DescribeTable(ctx, &req)
	if err != nil {
		return
	}
	tableName = resp.Info.GetName()
	sourceID = resp.Info.GetSourceId()
	tableSchema = resp.Info.GetTableSchema()

	return
}

func (s *SourceClient) DescribeSourceManager(ctx context.Context, ID string) (sourceType model.DataSource_Type, url *datasourcepb.DataSourceURL, err error) {
	var (
		req  request.DescribeSource
		resp *response.DescribeSource
	)

	req.SourceId = ID
	resp, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	sourceType = resp.Info.GetSourceType()
	url = resp.Info.GetUrl()

	return
}
