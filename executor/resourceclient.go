package executor

import (
	"context"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/respb"

	"github.com/DataWorkbench/common/grpcwrap"
)

type ResourceClient struct {
	client respb.ResourceClient
}

func NewFileClient(conn *grpcwrap.ClientConn) (c ResourceClient, err error) {
	c.client = respb.NewResourceClient(conn)
	return c, nil
}

func (s *ResourceClient) GetFileById(ctx context.Context, id string) (name string, url string, err error) {
	res, err := s.client.DescribeFile(ctx, &request.DescribeFile{ResourceId: id})
	if err != nil {
		return
	}
	name = res.Name
	spaceId := res.SpaceId
	url = "/" + spaceId + "/" + id + ".jar"
	return
}
