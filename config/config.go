package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/common/trace"
	"github.com/DataWorkbench/loader"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

// The config file path used by Load config
var FilePath string

const (
	envPrefix = "JOB_DEVELOPER"
)

type Config struct {
	LogLevel                 int8                   `json:"log_level"      yaml:"log_level"      env:"LOG_LEVEL,default=1" validate:"gte=1,lte=5"`
	GRPCServer               *grpcwrap.ServerConfig `json:"grpc_server"    yaml:"grpc_server"    env:"GRPC_SERVER"         validate:"required"`
	MetricsServer            *metrics.Config        `json:"metrics_server" yaml:"metrics_server" env:"METRICS_SERVER"      validate:"required"`
	ZeppelinFlinkHome        string                 `json:"zeppelin_flink_home"      yaml:"zeppelin_flink_home"      env:"ZEPPELIN_FLINK_HOME" validate:"required"`
	ZeppelinFlinkExecuteJars string                 `json:"zeppelin_flink_execute_jars"      yaml:"zeppelin_flink_execute_jars"      env:"ZEPPELIN_FLINK_EXECUTE_JARS" validate:"required"`
	SourcemanagerServer      string                 `json:"sourcemanager_server"      yaml:"sourcemanager_server"      env:"SOURCEMANAGER_SERVER" validate:"required"`
	UdfmanagerServer         string                 `json:"udfmanager_server"      yaml:"udfmanager_server"      env:"UDFMANAGER_SERVER" validate:"required"`
	Tracer                   *trace.Config          `json:"tracer"         yaml:"tracer"         env:"TRACER"              validate:"required"`
}

func loadFromFile(cfg *Config) (err error) {
	if FilePath == "" {
		return
	}

	fmt.Printf("%s load config from file <%s>\n", time.Now().Format(time.RFC3339Nano), FilePath)

	var b []byte
	b, err = ioutil.ReadFile(FilePath)
	if err != nil && os.IsNotExist(err) {
		return
	}

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		fmt.Println("parse config file error:", err)
	}
	return
}

func Load() (cfg *Config, err error) {
	cfg = &Config{}

	_ = loadFromFile(cfg)

	l := loader.New(
		loader.WithPrefix(envPrefix),
		loader.WithTagName("env"),
		loader.WithOverride(true),
	)
	if err = l.Load(cfg); err != nil {
		return
	}

	// output the config content
	fmt.Printf("%s pid=%d the latest configuration: \n", time.Now().Format(time.RFC3339Nano), os.Getpid())
	fmt.Println("")
	b, _ := yaml.Marshal(cfg)
	fmt.Println(string(b))

	validate := validator.New()
	if err = validate.Struct(cfg); err != nil {
		return
	}

	return
}
