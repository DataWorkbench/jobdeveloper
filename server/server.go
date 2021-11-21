package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataWorkbench/common/gtrace"
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
		tracer            gtrace.Tracer
		tracerCloser      io.Closer
		sourceManagerConn *grpcwrap.ClientConn
		sourceClient      executor.SourceClient
		UdfManagerConn    *grpcwrap.ClientConn
		udfClient         executor.UdfClient
		resourceManagerConn   *grpcwrap.ClientConn
		resourceClient        executor.ResourceClient
		engineManagerConn *grpcwrap.ClientConn
		engineClient      executor.EngineClient
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)
		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	tracer, tracerCloser, err = gtrace.New(cfg.Tracer)
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

	engineManagerConn, err = grpcwrap.NewConn(ctx, cfg.EnginemanagerServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	engineClient, err = executor.NewEngineClient(engineManagerConn)
	if err != nil {
		return
	}

	resourceManagerConn, err = grpcwrap.NewConn(ctx, cfg.ResourcemanagerServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	resourceClient, err = executor.NewFileClient(resourceManagerConn)
	if err != nil {
		return
	}

	// init grpc.Server
	rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer, grpcwrap.ServerWithTracer(tracer))
	if err != nil {
		return
	}
	rpcServer.Register(func(s *grpc.Server) {
		jobdevpb.RegisterJobdeveloperServer(s, NewJobDeveloperServer(executor.NewJobDeveloperExecutor(lp, engineClient, sourceClient, udfClient, resourceClient, cfg.ZeppelinFlinkHome, cfg.ZeppelinHadoopConf, cfg.ZeppelinFlinkExecuteJars)))
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
