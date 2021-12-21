package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"github.com/DataWorkbench/gproto/pkg/flinkpb"
	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

/*
mysql> desc ms;
+-------+--------+------+-----+---------+-------+
| Field | Type   | Null | Key | Default | Extra |
+-------+--------+------+-----+---------+-------+
| id    | bigint | YES  |     | NULL    |       |
| id1   | bigint | YES  |     | NULL    |       |
+-------+--------+------+-----+---------+-------+
2 rows in set (0.01 sec)

flink--table define
drop table if exists ms;
create table ms
(id bigint,id1 bigint) WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',
'table-name' = 'ms',
'username' = 'root',
'password' = '123456'
);
*/

// from mysql to mysql
var source_dest request.JobParser

// from hdfs to hdfs
var hdfs_source_dest request.JobParser

// from kafka to kafka
var kafka_source_dest request.JobParser

// from s3 to s3
var s3_source_dest request.JobParser

// from hbase to hbase
var hbase_source_dest request.JobParser

// from pg to pg
var pg_source_dest request.JobParser

// from ck to ck
var ck_source_dest request.JobParser

var source_rsource_join_dest_1 request.JobParser
var sql request.JobParser
var python request.JobParser
var scala request.JobParser
var java request.JobParser
var javafree request.JobFree

//var udf_values_dest request.JobParser
//var const_dest request.JobParser
//var source_orderby_dest request.JobParser
//var source_limit_offset_dest request.JobParser
//var source_offset_fetch_dest request.JobParser
//var source_filter_dest request.JobParser
//var source_source_filter_dest request.JobParser
//var source_source_union_filter_dest request.JobParser
//var source_source_except_filter_dest request.JobParser
//var source_source_intersect_filter_dest request.JobParser
//var source_groupby_dest request.JobParser
//var source_groupby_having_dest request.JobParser
//var source_window_dest request.JobParser
//var source_rsource_join_dest request.JobParser
//var source_udtf_join_dest request.JobParser
//var source_arrays_join_dest request.JobParser
//var source_udttf_join_dest request.JobParser
//var source_udttf_filter_dest request.JobParser
//var source_rsource_filter_in_dest request.JobParser
//var source_rsource_filter_exists_dest request.JobParser
//var source_rsource_join_jsource_join_dest request.JobParser
//var source_rsource_join_filter_dest request.JobParser

var client jobdevpb.JobdeveloperClient
var ctx context.Context
var initDone bool

func mainInit(t *testing.T) {
	if initDone == true {
		return
	}
	initDone = true
	spaceid := "wks-0000000000000001"
	sql = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-0000000000000sql", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_SQL, Sql: &flinkpb.FlinkSQL{Code: "drop table if exists md;\ncreate table md\n(id bigint,id1 bigint) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\n'table-name' = 'md',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ndrop table if exists ms;\ncreate table ms\n(id bigint,id1 bigint) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\n'table-name' = 'ms',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ninsert into md(id,id1) select ALL id as id,id1 from ms \n"}}}}
	python = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-0000000000python", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Python, Python: &flinkpb.FlinkPython{Code: "print(\"hello world\")"}}}}
	scala = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-00000000000scala", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Scala, Scala: &flinkpb.FlinkScala{Code: "println(\"hello world\")"}}}}
	java = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-000000000000java", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Jar, Jar: &flinkpb.FlinkJar{ResourceId: "rsm-0000000000000jar"}}}}
	javafree = request.JobFree{Resources: &model.JobResources{JobId: "job-000000000000java", Jar: "rsm-0000000000000jar"}}
	//source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-00000source_dest", SpaceId: spaceid, Args: &model.StreamJobArgs{Function: &model.StreamJobArgs_Function{UdfIds: []string{"udf-0000scalaplusone"}}}, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000mysqlsource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "plus_one(id)"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000mysqldest", Columns: []string{"id", "id1"}}}}}}}}
	hdfs_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-hdfs_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000hdfs_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000hdfs_dest", Columns: []string{"id", "id1"}}}}}}}}
	pg_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-00pg_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0postgres_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000postgres_dest", Columns: []string{"id", "id1"}}}}}}}}
	ck_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-00ck_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-clickhousesource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0clickhouse_dest", Columns: []string{"id", "id1"}}}}}}}}
	kafka_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-kafkasource_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000kafkasource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "paycount"}, &flinkpb.ColumnAs{Field: "paymoney"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000kafkadest", Columns: []string{"paycount", "paymoney"}}}}}}}}
	s3_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-00s3_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0000000s3_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000000000s3_dest", Columns: []string{"id", "id1"}}}}}}}}
	hbase_source_dest = request.JobParser{Command: constants.JobCommandRun, Job: &request.JobInfo{JobId: "job-hbasesource_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0000hbase_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "rowkey"}, &flinkpb.ColumnAs{Field: "columna"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000000hbase_dest", Columns: []string{"rowkey", "columna"}}}}}}}}

	//source_dest = request.JobParser{Command: "run", Job: &request.JobInfo{JobID: "job-0123456789012345", SpaceID: spaceid, Env: &model.StreamFlowEnv{EngineId: "eng-0123456789012345", JobCu: 1, TaskCu: 1, TaskNum: 1, Flink: &model.FlinkConfig{Parallelism: 1}}, Nodes: &model.FlinkJobNodes{JobNodes: []*model.FlinkDagNode{&model.FlinkDagNode{NodeType: "Source", NodeID: "xx0", DownStream: "xx1", PointX: "x", PointY: "y", Property: &model.FlinkNodeProperty{Source: &model.SourceNodeProperty{TableID: "sot-0123456789012347", Column: []*model.ColumnAs{&model.ColumnAs{Field: "id"}, &model.ColumnAs{Field: "id1"}}}}}, &model.FlinkDagNode{NodeType: "Dest", NodeID: "xx1", UpStream: "xx0", PointX: "x", PointY: "y", Property: &model.FlinkNodeProperty{Dest: &model.DestNodeProperty{TableID: "sot-0123456789012348", Columns: []string{"id", "id1"}}}}}}}}
	//source_dest = request.JobParser{Command: "run", Job: &request.JobInfo{JobID: "job-0123456789012345", SpaceID: spaceid, Env: &model.StreamFlowEnv{EngineId: "eng-0123456789012345", JobCu: 1, TaskCu: 1, TaskNum: 1, Flink: &model.FlinkConfig{Parallelism: 1}}, Nodes: &model.FlinkJobNodes{JobNodes: []*model.FlinkDagNode{&model.FlinkDagNode{NodeType: "Source", NodeID: "xx0", DownStream: "xx1", PointX: "x", PointY: "y", Property: &model.FlinkNodeProperty{Source: &model.SourceNodeProperty{TableID: "sot-0123456789012347", Column: []*model.ColumnAs{&model.ColumnAs{Field: "id"}, &model.ColumnAs{Field: "id1"}}}}}, &model.FlinkDagNode{NodeType: "Dest", NodeID: "xx1", UpStream: "xx0", PointX: "x", PointY: "y", Property: &model.FlinkNodeProperty{Dest: &model.DestNodeProperty{TableID: "sot-0123456789012348", Columns: []string{"id", "id1"}}}}}}}}
	//source_rsource_join_dest_1 = request.JobParser{ID: "01234567890123456789", WorkspaceID: "01234567890123456789", EngineID: "01234567890123456789", EngineType: "flink", Command: "run", JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012346","table":"billing as k","distinct":"ALL","column":[{"field":"k.paycount * r.rate","as":"stotal"}]}},{"nodetype":"Source","nodeid":"rxx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012355","table":"mw FOR SYSTEM_TIME AS OF k.tproctime AS r","distinct":"ALL","column":null}},{"nodetype":"Join","nodeid":"xx1","upstream":"xx0","upstreamright":"rxx0","downstream":"xx2","pointx":"","pointy":"","property":{"join":"JOIN","expression":"r.dbmoney = k.paymoney","column":[{"field":"stotal","as":""}]}},{"nodetype":"Dest","nodeid":"xx2","upstream":"xx1","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"mwd","column":["total"],"id":"sot-0123456789012356"}}]}`}
	//udf_values_dest = request.JobParser{ID: "xxxxxxxxxxxxxxxxxxxx", WorkspaceID: "xxxxxxxxxxxxxxxxxxxx", EngineID: "xxxxxxxxxxxxxxxxxxxx", EngineType: "flink", JobInfo: `{"command":"","stream_sql":false,"parallelism":0,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"nodes":"[{\"nodetype\":\"UDF\", \"nodeid\":\"xx0\", \"upstream\":\"\", \"downstream\":\"xx1\", \"property\":\"{\\\"id\\\":\\\"udf-xx0\\\"}\"}, {\"nodetype\":\"Values\", \"nodeid\":\"xx1\", \"upstream\":\"xx0\", \"downstream\":\"xx2\", \"property\":\"{\\\"row\\\": [\\\"1,2\\\", \\\"3,4\\\"]}\"}, {\"nodetype\":\"Dest\", \"nodeid\":\"xx2\", \"upstream\":\"xx1\", \"downstream\":\"\", \"property\":\"{\\\"table\\\":\\\"pd\\\", \\\"column\\\": [\\\"id\\\", \\\"id1\\\"], \\\"id\\\":\\\"sot-xx2\\\"}\"}]"}`}
	//const_dest = request.JobParser{JobInfo: `[{"nodetype":"Const", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"column\": [{\"field\":\"1\", \"as\":\"a\" }, {\"field\":\"2\", \"as\":\"b\" }]}"}, {"nodetype":"Dest", "nodeid":"xx1", "upstream":"xx0", "downstream":"", "property":"{\"table\":\"pd\", \"column\": [\"id\", \"id1\"], \"id\":\"sot-xx1\"}"}]`}
	//source_orderby_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"OrderBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"column\": [{\"field\":\"id\", \"order\":\"asc\" }, {\"field\":\"id1\", \"order\":\"desc\" }]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_limit_offset_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Limit", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"limit\":1}"}, {"nodetype":"Offset", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"offset\":2}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_offset_fetch_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Offset", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"offset\":1}"}, {"nodetype":"Fetch", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"fetch\":2}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"filter\":\"id > 2\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_source_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"filter\":\"ms.id = mss.id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_source_union_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Union", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"all\":\"all\"}"}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_source_except_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Except", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":""}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_source_intersect_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Intersect", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":""}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_groupby_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"sum(id1)\", \"as\":\"s\" }]}"}, {"nodetype":"GroupBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"groupby\": [\"id\"]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_groupby_having_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\", \"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"sum(id1)\", \"as\":\"s\" }]}"}, {"nodetype":"GroupBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"groupby\": [\"id\"]}"},{"nodetype":"Having", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"having\": \"sum(id1) > 7\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_window_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"count(id1) over w\", \"as\":\"\" }]}"}, {"nodetype":"Window", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"window\": [{\"name\": \"w\", \"spec\":\"order by id\"}]}"},  {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_rsource_join_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_udtf_join_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"UDTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"unnest_udtf\", \"args\":\"tags\", \"as\":\"t\", \"column\": [{\"field\":\"tag\"}], \"selectcolumn\": [{\"field\":\"tag\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"TRUE\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"tag\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\", \"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_arrays_join_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"Arrays", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"args\":\"tags\", \"as\":\"t\", \"column\": [{\"field\":\"tag\"}], \"selectcolumn\": [{\"field\":\"tag\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"CROSS JOIN\", \"expression\":\"\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"tag\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\", \"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_udttf_join_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"UDTTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"Rates\", \"args\":\"o_proctime\", \"column\": [{\"field\":\"r_rate\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"r_currency = msid\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"r_rate\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_udttf_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"UDTTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"Rates\", \"args\":\"o_proctime\", \"column\": [{\"field\":\"r_rate\"}]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"filter\":\"r_currency = msid\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_rsource_filter_in_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"},{"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"in\":\"id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_rsource_filter_exists_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"},{"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"exists\":\"id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_rsource_join_jsource_join_dest = request.JobParser{JobInfo: `[{"nodetype":"Source","nodeid":"xx0","upstream":"","downstream":"j1","property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"},{"nodetype":"Source","nodeid":"rxx0","upstream":"","downstream":"j1","property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"},{"nodetype":"Join","nodeid":"j1","upstream":"xx0","upstreamright":"rxx0","downstream":"j2","property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"},{"nodetype":"Source","nodeid":"jxx0","upstream":"","downstream":"j2","property":"{\"id\":\"sot-jxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"jmsid\" }]}"},{"nodetype":"Join","nodeid":"j2","upstream":"jxx0","upstreamright":"j1","downstream":"xxd","property":"{\"join\":\"JOIN\", \"expression\":\"j2right.msid = ms.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"},{\"field\":\"jmsid\"}]}"},{"nodetype":"Dest", "nodeid":"xxd","upstream":"j2","downstream":"","property":"{\"id\":\"sot-xxd\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	//source_rsource_join_filter_dest = request.JobParser{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"filter\":\"msid = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}

	address := "127.0.0.1:9109"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
	})
	require.Nil(t, err, "%+v", err)
	client = jobdevpb.NewJobdeveloperClient(conn)

	logger := glog.NewDefault()
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
}

func errorCode(err error) string {
	//rpc error: code = Unknown desc = InvalidSourceName
	return strings.Split(err.Error(), " ")[7]
}

func Test_OperatorRelations(t *testing.T) {
	mainInit(t)
	var req model.EmptyStruct

	_, err := client.NodeRelations(ctx, &req)
	require.Nil(t, err, "%+v", err)
	//require.Equal(t, "", resp.Relations)
}

func Test_JobParser(t *testing.T) {
	mainInit(t)
	var (
		err error
		//sql *response.JobParser
	)

	_, err = client.JobParser(ctx, &sql)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &python)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &scala)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &java)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &kafka_source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &hdfs_source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &s3_source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &hbase_source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &pg_source_dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.JobParser(ctx, &ck_source_dest)
	require.Nil(t, err, "%+v", err)
	//sql, err = client.JobParser(ctx, &source_dest)
	//_, err = client.JobParser(ctx, &source_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "{\"conf\":\"%flink.conf\\n\\nFLINK_HOME /home/lzzhang/bigdata/flink-bin-download/flink-1.11.2\\nflink.execution.mode remote\\nflink.execution.remote.host 127.0.0.1\\nflink.execution.remote.port 8081\\nzeppelin.flink.concurrentStreamSql.max 1000000\\nzeppelin.flink.concurrentBatchSql.max 1000000\\nflink.execution.jars /home/lzzhang/bigdata/flink-bin-download/zeppelin-0.9.0-bin-all/lib/flink-connector-jdbc_2.11-1.11.2.jar,/home/lzzhang/bigdata/flink-bin-download/zeppelin-0.9.0-bin-all/lib/mysql-connector-java-8.0.21.jar\\n\\n\",\"depends\":\"%flink.ssql(parallelism=2)\\n\\ndrop table if exists pd;\\ncreate table pd\\n(id bigint,id1 bigint) WITH (\\n'connector' = 'jdbc',\\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\\n'table-name' = 'pd',\\n'username' = 'root',\\n'password' = '123456'\\n);\\n\\n\\ndrop table if exists ms;\\ncreate table ms\\n(id bigint,id1 bigint) WITH (\\n'connector' = 'jdbc',\\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\\n'table-name' = 'ms',\\n'username' = 'root',\\n'password' = '123456'\\n);\\n\\n\\n\",\"funcscala\":\"\",\"mainrun\":\"%flink.ssql(parallelism=2)\\n\\ninsert into pd(id,id1)  select ALL id as id,id1 from ms \",\"s3\":{\"accesskey\":\"\",\"secretkey\":\"\",\"endpoint\":\"\"},\"resource\":{\"jar\":\"\",\"jobid\":\"01234567890123456789\",\"engineID\":\"01234567890123456789\"}}", sql.GetJobElement())

	//sql, err = client.JobParser(ctx, &source_rsource_join_dest_1)
	//require.Nil(t, err, "%+v", err)

	//sql, err = client.JobParser(ctx, &udf_values_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  values(1,2),(3,4)", sql.JobElement)

	//sql, err = client.JobParser(ctx, &udf_values_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  values(1,2),(3,4)", sql.Sql)
	//require.Equal(t, "sot-xx2", sql.Table)
	//require.Equal(t, "udf-xx0", sql.UDF)

	//sql, err = client.JobParser(ctx, &const_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select xx0.a,xx0.b from ( select 1 as a,2 as b) as xx0 ", sql.Sql)
	//require.Equal(t, "sot-xx1", sql.Table)

	//sql, err = client.JobParser(ctx, &source_orderby_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  order by id asc,id1 desc ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_limit_offset_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  limit 1  offset 2 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_offset_fetch_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  offset 1  fetch first 2 rows only ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  where id > 2 ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_source_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL ms.id as msid,mss.id1 as mssid1 from ms, mss  where ms.id = mss.id ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_source_union_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select id,id1 from ((select ALL ms.id as id,ms.id1 as id1 from ms )  union all ( select ALL mss.id as mssid,mss.id1 as mssid1 from mss ))  where id = 1 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_source_except_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select id,id1 from ((select ALL ms.id as id,ms.id1 as id1 from ms )  except ( select ALL mss.id as mssid,mss.id1 as mssid1 from mss ))  where id = 1 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_source_intersect_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select id,id1 from ((select ALL ms.id as id,ms.id1 as id1 from ms )  intersect ( select ALL mss.id as mssid,mss.id1 as mssid1 from mss ))  where id = 1 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_groupby_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,sum(id1) as s from ms  group by id ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_groupby_having_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,sum(id1) as s from ms  group by id  having sum(id1) > 7 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_window_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,count(id1) over w from ms  window  w as (order by id) ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_rsource_join_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,mssid1 from ( select ALL ms.id as msid,mss.id1 as mssid1 from ms JOIN mss ON ms.id = mss.id)", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_udtf_join_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,tag from ( select ALL ms.id as msid,ms.id1 as msid1,tag from ms JOIN  LATERAL TABLE(unnest_udtf(tags)) AS t(tag) ON TRUE)", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)
	//require.Equal(t, "udf-rxx0", sql.UDF)

	//sql, err = client.JobParser(ctx, &source_arrays_join_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,tag from ( select ALL ms.id as msid,ms.id1 as msid1,tag from ms CROSS JOIN UNNEST(tags)) AS t(tag))", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_udttf_join_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,r_rate from ( select ALL ms.id as msid,ms.id1 as msid1,r_rate from ms JOIN  LATERAL TABLE(Rates(o_proctime)) ON r_currency = msid)", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)
	//require.Equal(t, "udf-rxx0", sql.UDF)

	//sql, err = client.JobParser(ctx, &source_udttf_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL ms.id as msid,r_rate from ms,  LATERAL TABLE(Rates(o_proctime))  where r_currency = msid ", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-xx0", sql.Table)
	//require.Equal(t, "udf-rxx0", sql.UDF)

	//sql, err = client.JobParser(ctx, &source_rsource_filter_in_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  where id in ( select ALL id as id from ms )", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-rxx0,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_rsource_filter_exists_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select ALL id as id,id1 from ms  where id exists ( select ALL id as id from ms )", sql.Sql)
	//require.Equal(t, "sot-xx2,sot-rxx0,sot-xx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_rsource_join_jsource_join_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,mssid1,jmsid from ( select ALL ms.id as jmsid,msid,mssid1 from ms JOIN  ( select ALL msid,mssid1 from  ( select ALL ms.id as msid,mss.id1 as mssid1 from ms JOIN mss ON ms.id = mss.id)  ) as j2right ON j2right.msid = ms.id)", sql.Sql)
	//require.Equal(t, "sot-xxd,sot-jxx0,sot-xx0,sot-rxx0", sql.Table)

	//sql, err = client.JobParser(ctx, &source_rsource_join_filter_dest)
	//require.Nil(t, err, "%+v", err)
	//require.Equal(t, "insert into pd(id,id1)  select msid,mssid1 from ( select ALL ms.id as msid,mss.id1 as mssid1 from ms JOIN mss ON ms.id = mss.id) where msid = 1 ", sql.Sql)
	//require.Equal(t, "sot-xx3,sot-xx0,sot-rxx0", sql.Table)
}

func Test_JobFree(t *testing.T) {
	mainInit(t)
	var (
		err error
		//sql *response.JobParser
	)

	_, err = client.JobFree(ctx, &javafree)
	require.Nil(t, err, "%+v", err)
}
