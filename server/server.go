package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataWorkbench/common/trace"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/glog"
	"google.golang.org/grpc"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/gproto/pkg/jobdevpb"

	"github.com/DataWorkbench/jobdeveloper/config"
	"github.com/DataWorkbench/jobdeveloper/executor"
)

// Start for start the http server
func Start() (err error) {
	fmt.Printf("%s pid=%d program_build_info: %s\n",
		time.Now().Format(time.RFC3339Nano), os.Getpid(), buildinfo.JSONString)

	var cfg *config.Config

	cfg, err = config.Load()
	if err != nil {
		return
	}

	// init parent logger
	lp := glog.NewDefault().WithLevel(glog.Level(cfg.LogLevel))
	ctx := glog.WithContext(context.Background(), lp)

	var (
		rpcServer         *grpcwrap.Server
		metricServer      *metrics.Server
		tracer            trace.Tracer
		tracerCloser      io.Closer
		sourceManagerConn *grpcwrap.ClientConn
		sourceClient      executor.SourceClient
		UdfManagerConn    *grpcwrap.ClientConn
		udfClient         executor.UdfClient
		fileManagerConn   *grpcwrap.ClientConn
		fileClient        executor.FileClient
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)
		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	tracer, tracerCloser, err = trace.New(cfg.Tracer)
	if err != nil {
		return
	}

	sourceManagerConn, err = grpcwrap.NewConn(ctx, cfg.SourcemanagerServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	sourceClient, err = executor.NewSourceClient(sourceManagerConn)
	if err != nil {
		return
	}

	UdfManagerConn, err = grpcwrap.NewConn(ctx, cfg.UdfmanagerServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	udfClient, err = executor.NewUdfClient(UdfManagerConn)
	if err != nil {
		return
	}

	fileManagerConn, err = grpcwrap.NewConn(ctx, cfg.FilemanagerServer, grpcwrap.ClientWithTracer(tracer))
	if err !=nil{
		return
	}

	//TODO
	fileClient, err = executor.NewFileClient(fileManagerConn)
	if err!=nil{
		return
	}

	// init grpc.Server
	rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer, grpcwrap.ServerWithTracer(tracer))
	if err != nil {
		return
	}
	rpcServer.Register(func(s *grpc.Server) {
		jobdevpb.RegisterJobdeveloperServer(s, NewJobDeveloperServer(executor.NewJobDeveloperExecutor(lp, sourceClient, udfClient,fileClient, cfg.ZeppelinFlinkHome, cfg.ZeppelinFlinkExecuteJars)))
	})

	// handle signal
	sigGroup := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
	sigChan := make(chan os.Signal, len(sigGroup))
	signal.Notify(sigChan, sigGroup...)

	blockChan := make(chan struct{})

	// run grpc server
	go func() {
		err = rpcServer.ListenAndServe()
		blockChan <- struct{}{}
	}()

	// init prometheus server
	metricServer, err = metrics.NewServer(ctx, cfg.MetricsServer)
	if err != nil {
		return err
	}

	go func() {
		if err := metricServer.ListenAndServe(); err != nil {
			return
		}
	}()

	go func() {
		sig := <-sigChan
		lp.Info().String("receive system signal", sig.String()).Fire()
		blockChan <- struct{}{}
	}()

	<-blockChan
	return
}
