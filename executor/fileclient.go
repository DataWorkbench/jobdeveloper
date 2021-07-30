package executor

import (
	"context"

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

func (s *FileClient) GetFileById(ctx context.Context, ID string) (name string, path string, err error) {
	/*
		res, err := s.client.GetFileById(ctx, &fmpb.IdRequest{ID: ID})
		if err != nil {
			return
		}
		name = res.Name
		path = res.URL
	*/
	return
}
