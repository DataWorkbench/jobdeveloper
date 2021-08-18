package executor

import (
	"context"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
)

type FileClient struct {
	client fmpb.FileManagerClient
}

func NewFileClient(conn *grpcwrap.ClientConn) (c FileClient, err error) {
	c.client = fmpb.NewFileManagerClient(conn)
	return c, nil
}

func (s *FileClient) GetFileById(ctx context.Context, id string) (name string, url string, err error) {
	res, err := s.client.DescribeFile(ctx, &fmpb.DescribeRequest{FileId: id})
	if err != nil {
		return
	}
	name = res.FileName
	url = res.Url
	return
}
