package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/model"
)

/*
mysql--table define

mysql> desc ms;
+-------+--------+------+-----+---------+-------+
| Field | Type   | Null | Key | Default | Extra |
+-------+--------+------+-----+---------+-------+
| id    | bigint | YES  |     | NULL    |       |
| id1   | bigint | YES  |     | NULL    |       |
+-------+--------+------+-----+---------+-------+
2 rows in set (0.01 sec)

mysql> desc mss;
+-------+--------+------+-----+---------+-------+
| Field | Type   | Null | Key | Default | Extra |
+-------+--------+------+-----+---------+-------+
| id    | bigint | YES  |     | NULL    |       |
| id1   | bigint | YES  |     | NULL    |       |
+-------+--------+------+-----+---------+-------+
2 rows in set (0.00 sec)

mysql> desc pd;
+-------+--------+------+-----+---------+-------+
| Field | Type   | Null | Key | Default | Extra |
+-------+--------+------+-----+---------+-------+
| id    | bigint | YES  |     | NULL    |       |
| id1   | bigint | YES  |     | NULL    |       |
+-------+--------+------+-----+---------+-------+
3 rows in set (0.01 sec)

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

drop table if exists mss;
create table mss
(id bigint,id1 bigint) WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',
'table-name' = 'mss',
'username' = 'root',
'password' = '123456'
);

drop table if exists pd;
create table pd
(id bigint primary key NOT ENFORCED,id1 bigint) WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',
'table-name' = 'pd',
'username' = 'root',
'password' = '123456'
);

-- insert into union, pd must have primary key.
*/

var source_dest jobdevpb.JobParserRequest
var udf_values_dest jobdevpb.JobParserRequest
var const_dest jobdevpb.JobParserRequest
var source_orderby_dest jobdevpb.JobParserRequest
var source_limit_offset_dest jobdevpb.JobParserRequest
var source_offset_fetch_dest jobdevpb.JobParserRequest
var source_filter_dest jobdevpb.JobParserRequest
var source_source_filter_dest jobdevpb.JobParserRequest
var source_source_union_filter_dest jobdevpb.JobParserRequest
var source_source_except_filter_dest jobdevpb.JobParserRequest
var source_source_intersect_filter_dest jobdevpb.JobParserRequest
var source_groupby_dest jobdevpb.JobParserRequest
var source_groupby_having_dest jobdevpb.JobParserRequest
var source_window_dest jobdevpb.JobParserRequest
var source_rsource_join_dest jobdevpb.JobParserRequest
var source_udtf_join_dest jobdevpb.JobParserRequest
var source_arrays_join_dest jobdevpb.JobParserRequest
var source_udttf_join_dest jobdevpb.JobParserRequest
var source_udttf_filter_dest jobdevpb.JobParserRequest
var source_rsource_filter_in_dest jobdevpb.JobParserRequest
var source_rsource_filter_exists_dest jobdevpb.JobParserRequest
var source_rsource_join_jsource_join_dest jobdevpb.JobParserRequest
var source_rsource_join_filter_dest jobdevpb.JobParserRequest

var client jobdevpb.JobdeveloperClient
var ctx context.Context
var initDone bool

func mainInit(t *testing.T) {
	if initDone == true {
		return
	}
	initDone = true
	//source_dest = jobdevpb.JobParserRequest{ID: "01234567890123456789", WorkspaceID: "01234567890123456789", EngineID: "01234567890123456789", EngineType: "flink", JobInfo: constants.FlinkNodel{Nodes: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-0123456789012347\", \"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Dest", "nodeid":"xx1", "upstream":"xx0", "downstream":"", "property":"{\"id\":\"sot-0123456789012348\", \"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}}
	source_dest = jobdevpb.JobParserRequest{ID: "01234567890123456789", WorkspaceID: "01234567890123456789", EngineID: "01234567890123456789", EngineType: "flink", JobInfo: `{"command":"run","stream_sql":true,"parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	udf_values_dest = jobdevpb.JobParserRequest{ID: "xxxxxxxxxxxxxxxxxxxx", WorkspaceID: "xxxxxxxxxxxxxxxxxxxx", EngineID: "xxxxxxxxxxxxxxxxxxxx", EngineType: "flink", JobInfo: `{"command":"","stream_sql":false,"parallelism":0,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"nodes":"[{\"nodetype\":\"UDF\", \"nodeid\":\"xx0\", \"upstream\":\"\", \"downstream\":\"xx1\", \"property\":\"{\\\"id\\\":\\\"udf-xx0\\\"}\"}, {\"nodetype\":\"Values\", \"nodeid\":\"xx1\", \"upstream\":\"xx0\", \"downstream\":\"xx2\", \"property\":\"{\\\"row\\\": [\\\"1,2\\\", \\\"3,4\\\"]}\"}, {\"nodetype\":\"Dest\", \"nodeid\":\"xx2\", \"upstream\":\"xx1\", \"downstream\":\"\", \"property\":\"{\\\"table\\\":\\\"pd\\\", \\\"column\\\": [\\\"id\\\", \\\"id1\\\"], \\\"id\\\":\\\"sot-xx2\\\"}\"}]"}`}
	const_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Const", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"column\": [{\"field\":\"1\", \"as\":\"a\" }, {\"field\":\"2\", \"as\":\"b\" }]}"}, {"nodetype":"Dest", "nodeid":"xx1", "upstream":"xx0", "downstream":"", "property":"{\"table\":\"pd\", \"column\": [\"id\", \"id1\"], \"id\":\"sot-xx1\"}"}]`}
	source_orderby_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"OrderBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"column\": [{\"field\":\"id\", \"order\":\"asc\" }, {\"field\":\"id1\", \"order\":\"desc\" }]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_limit_offset_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Limit", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"limit\":1}"}, {"nodetype":"Offset", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"offset\":2}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_offset_fetch_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Offset", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"offset\":1}"}, {"nodetype":"Fetch", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"fetch\":2}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"filter\":\"id > 2\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_source_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"filter\":\"ms.id = mss.id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_source_union_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Union", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"all\":\"all\"}"}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_source_except_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Except", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":""}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_source_intersect_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"id\" }, {\"field\":\"ms.id1\", \"as\":\"id1\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id\", \"as\":\"mssid\" }, {\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Intersect", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":""}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "upstreamright":"", "downstream":"xx3", "property":"{\"filter\":\"id = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_groupby_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"sum(id1)\", \"as\":\"s\" }]}"}, {"nodetype":"GroupBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"groupby\": [\"id\"]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_groupby_having_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\", \"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"sum(id1)\", \"as\":\"s\" }]}"}, {"nodetype":"GroupBy", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"groupby\": [\"id\"]}"},{"nodetype":"Having", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"having\": \"sum(id1) > 7\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_window_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"count(id1) over w\", \"as\":\"\" }]}"}, {"nodetype":"Window", "nodeid":"xx1", "upstream":"xx0", "downstream":"xx2", "property":"{\"window\": [{\"name\": \"w\", \"spec\":\"order by id\"}]}"},  {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_rsource_join_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_udtf_join_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"UDTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"unnest_udtf\", \"args\":\"tags\", \"as\":\"t\", \"column\": [{\"field\":\"tag\"}], \"selectcolumn\": [{\"field\":\"tag\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"TRUE\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"tag\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\", \"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_arrays_join_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"Arrays", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"args\":\"tags\", \"as\":\"t\", \"column\": [{\"field\":\"tag\"}], \"selectcolumn\": [{\"field\":\"tag\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"CROSS JOIN\", \"expression\":\"\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"tag\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\", \"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_udttf_join_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }, {\"field\":\"ms.id1\", \"as\":\"msid1\" }]}"}, {"nodetype":"UDTTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"Rates\", \"args\":\"o_proctime\", \"column\": [{\"field\":\"r_rate\"}]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"r_currency = msid\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"r_rate\"}]}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_udttf_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"UDTTF", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"udf-rxx0\",\"funcname\":\"Rates\", \"args\":\"o_proctime\", \"column\": [{\"field\":\"r_rate\"}]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"filter\":\"r_currency = msid\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_rsource_filter_in_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"},{"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"in\":\"id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_rsource_filter_exists_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }, {\"field\":\"id1\", \"as\":\"\" }]}"},{"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"id\", \"as\":\"id\" }]}"}, {"nodetype":"Filter", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"exists\":\"id\"}"}, {"nodetype":"Dest", "nodeid":"xx2", "upstream":"xx1", "downstream":"", "property":"{\"id\":\"sot-xx2\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_rsource_join_jsource_join_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source","nodeid":"xx0","upstream":"","downstream":"j1","property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"},{"nodetype":"Source","nodeid":"rxx0","upstream":"","downstream":"j1","property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"},{"nodetype":"Join","nodeid":"j1","upstream":"xx0","upstreamright":"rxx0","downstream":"j2","property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"},{"nodetype":"Source","nodeid":"jxx0","upstream":"","downstream":"j2","property":"{\"id\":\"sot-jxx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"jmsid\" }]}"},{"nodetype":"Join","nodeid":"j2","upstream":"jxx0","upstreamright":"j1","downstream":"xxd","property":"{\"join\":\"JOIN\", \"expression\":\"j2right.msid = ms.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"},{\"field\":\"jmsid\"}]}"},{"nodetype":"Dest", "nodeid":"xxd","upstream":"j2","downstream":"","property":"{\"id\":\"sot-xxd\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}
	source_rsource_join_filter_dest = jobdevpb.JobParserRequest{JobInfo: `[{"nodetype":"Source", "nodeid":"xx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-xx0\",\"table\":\"ms\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"ms.id\", \"as\":\"msid\" }]}"}, {"nodetype":"Source", "nodeid":"rxx0", "upstream":"", "downstream":"xx1", "property":"{\"id\":\"sot-rxx0\",\"table\":\"mss\", \"distinct\":\"ALL\", \"column\": [{\"field\":\"mss.id1\", \"as\":\"mssid1\" }]}"}, {"nodetype":"Join", "nodeid":"xx1", "upstream":"xx0", "upstreamright":"rxx0", "downstream":"xx2", "property":"{\"join\":\"JOIN\", \"expression\":\"ms.id = mss.id\", \"column\": [{\"field\":\"msid\"}, {\"field\":\"mssid1\"}]}"}, {"nodetype":"Filter", "nodeid":"xx2", "upstream":"xx1", "downstream":"xx3", "property":"{\"filter\":\"msid = 1\"}"}, {"nodetype":"Dest", "nodeid":"xx3", "upstream":"xx2", "downstream":"", "property":"{\"id\":\"sot-xx3\",\"table\":\"pd\", \"column\": [\"id\", \"id1\"]}"}]`}

	address := "127.0.0.1:53001"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      address,
		LogLevel:     2,
		LogVerbosity: 99,
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

func Test_FlinkNodeRelations(t *testing.T) {
	mainInit(t)
	var req model.EmptyStruct

	resp, err := client.NodeRelations(ctx, &req)
	require.Nil(t, err, "%+v", err)
	//require.Equal(t, "", resp.Relations)

	require.Equal(t, "[{\"nodetype\":\"UDF\",\"allowupstream\":[\"Empty\",\"UDF\",\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"GroupBy\",\"Having\",\"Window\"],\"allowdownstream\":[\"Values\",\"Source\",\"Filter\",\"Const\"]},{\"nodetype\":\"Values\",\"allowupstream\":[\"Empty\",\"UDF\"],\"allowdownstream\":[\"Dest\"]},{\"nodetype\":\"Dest\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Const\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"Values\",\"Empty\"],\"allowdownstream\":[\"Empty\"]},{\"nodetype\":\"Const\",\"allowupstream\":[\"Empty\",\"UDF\"],\"allowdownstream\":[\"Dest\",\"Join\",\"Union\",\"Except\",\"Intersect\"]},{\"nodetype\":\"Source\",\"allowupstream\":[\"Empty\",\"UDF\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"UDTF\",\"Empty\"]},{\"nodetype\":\"OrderBy\",\"allowupstream\":[\"Source\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Dest\",\"UDF\"]},{\"nodetype\":\"Limit\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"UDF\",\"Dest\"]},{\"nodetype\":\"Offset\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"UDF\",\"Dest\"]},{\"nodetype\":\"Fetch\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Join\",\"Union\",\"Except\",\"Intersect\",\"UDF\",\"Dest\"]},{\"nodetype\":\"Filter\",\"allowupstream\":[\"Source\",\"UDF\",\"Const\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"UDTTF\"],\"allowdownstream\":[\"Dest\",\"GroupBy\",\"Having\",\"Window\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\"]},{\"nodetype\":\"Union\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Const\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"]},{\"nodetype\":\"Except\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Const\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"]},{\"nodetype\":\"Intersect\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Const\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"]},{\"nodetype\":\"GroupBy\",\"allowupstream\":[\"Source\",\"Filter\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Having\",\"Window\"]},{\"nodetype\":\"Having\",\"allowupstream\":[\"Source\",\"Filter\",\"GroupBy\",\"Join\",\"Union\",\"Except\",\"Intersect\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Window\"]},{\"nodetype\":\"Window\",\"allowupstream\":[\"Source\",\"Filter\",\"GroupBy\",\"Having\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\"]},{\"nodetype\":\"UDTF\",\"allowupstream\":[\"Empty\",\"Source\"],\"allowdownstream\":[\"Join\"]},{\"nodetype\":\"Join\",\"allowupstream\":[\"Source\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Const\",\"Filter\",\"GroupBy\",\"Having\",\"Window\",\"UDTF\",\"Arrays\",\"UDTTF\"],\"allowdownstream\":[\"Dest\",\"OrderBy\",\"Limit\",\"Offset\",\"Fetch\",\"Join\",\"Union\",\"Except\",\"Intersect\",\"Filter\",\"GroupBy\",\"Having\",\"Window\"]},{\"nodetype\":\"UDTTF\",\"allowupstream\":[\"Empty\",\"Source\"],\"allowdownstream\":[\"Join\",\"Filter\"]},{\"nodetype\":\"Arrays\",\"allowupstream\":[\"Empty\",\"Source\"],\"allowdownstream\":[\"Join\"]},{\"nodetype\":\"Sql\",\"allowupstream\":[\"Empty\"],\"allowdownstream\":[\"Empty\"]}]", resp.Relations)
}

func Test_JobParserRequestToSQL(t *testing.T) {
	mainInit(t)
	var (
		err error
		sql *jobdevpb.JobElement
	)
	sql, err = client.JobParser(ctx, &source_dest)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, "{\"conf\":\"%flink.conf\\n\\nFLINK_HOME /home/lzzhang/bigdata/flink-bin-download/flink-1.11.2\\nflink.execution.mode remote\\nflink.execution.remote.host 127.0.0.1\\nflink.execution.remote.port 8081\\nzeppelin.flink.concurrentStreamSql.max 1000000\\nzeppelin.flink.concurrentBatchSql.max 1000000\\nflink.execution.jars /home/lzzhang/bigdata/flink-bin-download/zeppelin-0.9.0-bin-all/lib/flink-connector-jdbc_2.11-1.11.2.jar,/home/lzzhang/bigdata/flink-bin-download/zeppelin-0.9.0-bin-all/lib/mysql-connector-java-8.0.21.jar\\n\\n\",\"depends\":\"%flink.ssql(parallelism=2)\\n\\ndrop table if exists pd;\\ncreate table pd\\n(id bigint,id1 bigint) WITH (\\n'connector' = 'jdbc',\\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\\n'table-name' = 'pd',\\n'username' = 'root',\\n'password' = '123456'\\n);\\n\\n\\ndrop table if exists ms;\\ncreate table ms\\n(id bigint,id1 bigint) WITH (\\n'connector' = 'jdbc',\\n'url' = 'jdbc:mysql://127.0.0.1:3306/data_workbench',\\n'table-name' = 'ms',\\n'username' = 'root',\\n'password' = '123456'\\n);\\n\\n\\n\",\"funcscala\":\"\",\"mainrun\":\"%flink.ssql(parallelism=2)\\n\\ninsert into pd(id,id1)  select ALL id as id,id1 from ms \",\"s3\":{\"accesskey\":\"\",\"secretkey\":\"\",\"endpoint\":\"\"},\"resource\":{\"jar\":\"\",\"jobid\":\"01234567890123456789\",\"engineID\":\"01234567890123456789\"}}", sql.GetJobElement())

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
