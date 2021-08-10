package executor

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/fmpb"
)

//TODO
type FileClient struct {
	client fmpb.FileManagerClient
}

func NewFileClient(conn *grpcwrap.ClientConn) (c FileClient, err error) {
	c.client = fmpb.NewFileManagerClient(conn)
	return c, nil
}

func (s *FileClient) GetFileById(ctx context.Context, id, spaceId string) (name string, url string, err error) {

	res, err := s.client.ListFiles(ctx, &fmpb.FilesFilterRequest{ID: id, SpaceID: spaceId})
	if err != nil {
		return
	} else if res.Total != 1 {
		err = status.Error(codes.InvalidArgument, "file not exist")
		return
	}
	name = res.Data[0].FileName
	url = res.Data[0].URL
	return
}
