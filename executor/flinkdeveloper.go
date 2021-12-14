package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/flinkpb"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
)

type OperatorRelation struct {
	OperatorType    flinkpb.FlinkOperator_Type   `json:"nodetype"`
	AllowUpstream   []flinkpb.FlinkOperator_Type `json:"allowupstream"`
	AllowDownStream []flinkpb.FlinkOperator_Type `json:"allowdownstream"`
}

var (
	nodeRelations  []OperatorRelation
	Quote          = "$qc$"
	UDFQuote       = Quote + "_udf_name_" + Quote
	FlinkHostQuote = Quote + "FLINK_HOST" + Quote
	FlinkPortQuote = Quote + "FLINK_PORT" + Quote
	SynatxCmd      = "java -jar /zeppelin/flink/depends/sql-validator.jar "
)

func GetSqlColumnDefine(sqlColumns []*flinkpb.SqlColumnType, timeColumns []*flinkpb.SqlTimeColumnType) (define string) {
	var primaryKey string
	var boolMap map[string]bool

	boolMap = make(map[string]bool)
	boolMap["on"] = true
	boolMap["off"] = false
	boolMap["true"] = true
	boolMap["false"] = false
	boolMap["yes"] = true
	boolMap["no"] = false
	boolMap["t"] = true
	boolMap["f"] = false
	boolMap["T"] = true
	boolMap["F"] = false
	boolMap["1"] = true
	boolMap["0"] = false

	for _, column := range sqlColumns {
		if define != "" {
			define += ", "
		}
		define += column.Column + " "
		define += column.Type
		if column.Length != "" && column.Length != "0" {
			define += "(" + column.Length + ")"
		}
		define += " "
		if column.Comment != "" {
			define += "COMMENT '" + column.Comment + "' "
		}

		if boolMap[column.PrimaryKey] == true {
			if primaryKey != "" {
				primaryKey += ", "
			}
			primaryKey += column.Column
		}
	}
	if timeColumns != nil {
		for _, column := range timeColumns {
			if define != "" {
				define += ", "
			}

			if column.Type == flinkpb.SqlTimeColumnType_Proctime {
				define += column.Column + " AS PROCTIME() "
			} else if column.Type == flinkpb.SqlTimeColumnType_Watermark {
				define += " WATERMARK FOR " + column.Column + " AS " + column.Expression + " "
			}
		}
	}
	if primaryKey != "" {
		define += ", PRIMARY KEY (" + primaryKey + ") NOT ENFORCED"
	}
	return
}

type SqlStack struct {
	TableId   []string
	UDFID     []string
	NodeCount int
	Standard  bool
	Sql       string
	Table     string
	Distinct  string
	Column    []flinkpb.ColumnAs
	TableAs   string
	Other     string
}

func In(haystack interface{}, needle interface{}) bool {
	sVal := reflect.ValueOf(haystack)
	kind := sVal.Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		for i := 0; i < sVal.Len(); i++ {
			if sVal.Index(i).Interface() == needle {
				return true
			}
		}

		return false
	}

	return false
}

func CheckFlinkOperatorRelations(operator []*flinkpb.FlinkOperator) (err error) {
	for _, o := range operator {
		var (
			relation          OperatorRelation
			upStreamNode      flinkpb.FlinkOperator
			upStreamRightNode flinkpb.FlinkOperator
			downStreamNode    flinkpb.FlinkOperator
		)

		relation, _, err = GetOperatorRelation(o.Type)
		if err != nil {
			return
		}
		upStreamNode, err = getOperatorNode(operator, o.Upstream)
		if err != nil {
			return
		}
		downStreamNode, err = getOperatorNode(operator, o.DownStream)
		if err != nil {
			return
		}

		if In(relation.AllowUpstream, upStreamNode.Type) == false {
			err = fmt.Errorf("this node type " + o.Type.String() + " only allow these upstream " + fmt.Sprintf("%#v", relation.AllowUpstream) + " not allow " + upStreamNode.Type.String())
		} else if In(relation.AllowDownStream, downStreamNode.Type) == false {
			err = fmt.Errorf("this node type " + o.Type.String() + " only allow these downstream " + fmt.Sprintf("%#v", relation.AllowDownStream) + " not allow " + downStreamNode.Type.String())
		} else {
			if o.Type == flinkpb.FlinkOperator_Join || o.Type == flinkpb.FlinkOperator_Union || o.Type == flinkpb.FlinkOperator_Except || o.Type == flinkpb.FlinkOperator_Intersect {
				upStreamRightNode, err = getOperatorNode(operator, o.UpstreamRight)
				if err != nil {
					return
				}

				if In(relation.AllowUpstream, upStreamRightNode.Type) == false {
					err = fmt.Errorf("this node type " + o.Type.String() + " only allow these upstream " + fmt.Sprintf("%#v", relation.AllowUpstream) + " not allow " + upStreamRightNode.Type.String())
				}
			}
		}
		if err != nil {
			return
		}

	}
	return
}

func getOperatorNode(operator []*flinkpb.FlinkOperator, nodeID string) (node flinkpb.FlinkOperator, err error) {
	if nodeID == "" {
		node = flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Empty}
	} else {
		var found bool

		for _, o := range operator {
			if o.Id == nodeID {
				node = *o
				found = true
				break
			}
		}

		if found == false {
			err = fmt.Errorf("can't find the nodeID " + nodeID)
		}
	}

	return
}

func getOperatorNodeByType(operator []*flinkpb.FlinkOperator, nodeType flinkpb.FlinkOperator_Type) (node flinkpb.FlinkOperator, err error) {
	var found bool

	for _, o := range operator {
		if o.Type == nodeType {
			node = *o
			found = true
		}
	}

	if found == false {
		err = fmt.Errorf("can't find the nodeType " + nodeType.String())
	}

	return
}

func GetOperatorRelation(nodeType flinkpb.FlinkOperator_Type) (nodeRelation OperatorRelation, jsonRelation string, err error) {
	var found bool

	if len(nodeRelations) == 0 {
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Values,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Dest,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_Values, flinkpb.FlinkOperator_Empty}, // upstream EmptyNode used for sql preview
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Const,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Dimension,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_Join}}) //downstream EmptyNode used for sql preview
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Source,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_UDTF, flinkpb.FlinkOperator_Empty}}) //downstream EmptyNode used for sql preview
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_OrderBy,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Dest}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Limit,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Dest}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Offset,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Dest}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Fetch,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Dest}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Filter,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_UDTTF},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Union,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Except,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Intersect,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_GroupBy,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Having}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Having,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_UDTF,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty, flinkpb.FlinkOperator_Source},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Join}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_Join,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Source, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Const, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having, flinkpb.FlinkOperator_UDTF, flinkpb.FlinkOperator_UDTTF},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Dest, flinkpb.FlinkOperator_OrderBy, flinkpb.FlinkOperator_Limit, flinkpb.FlinkOperator_Offset, flinkpb.FlinkOperator_Fetch, flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Union, flinkpb.FlinkOperator_Except, flinkpb.FlinkOperator_Intersect, flinkpb.FlinkOperator_Filter, flinkpb.FlinkOperator_GroupBy, flinkpb.FlinkOperator_Having}})
		nodeRelations = append(nodeRelations, OperatorRelation{
			OperatorType:    flinkpb.FlinkOperator_UDTTF,
			AllowUpstream:   []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Empty, flinkpb.FlinkOperator_Source},
			AllowDownStream: []flinkpb.FlinkOperator_Type{flinkpb.FlinkOperator_Join, flinkpb.FlinkOperator_Filter}})
	}

	if nodeType == flinkpb.FlinkOperator_Empty {
		jsonByte, errtmp := json.Marshal(nodeRelations)
		if errtmp != nil {
			err = errtmp
			return
		}
		jsonRelation = string(jsonByte)
		found = true
	} else {
		for _, n := range nodeRelations {
			if n.OperatorType == nodeType {
				nodeRelation = n
				found = true
				break
			}
		}
	}

	if found == false {
		err = fmt.Errorf("can't find this nodeType " + nodeType.String())
	}

	return
}

func isSimpleTable(OperatorType flinkpb.FlinkOperator_Type) bool {
	if OperatorType == flinkpb.FlinkOperator_Source || OperatorType == flinkpb.FlinkOperator_Dimension || OperatorType == flinkpb.FlinkOperator_Const || OperatorType == flinkpb.FlinkOperator_UDTTF || OperatorType == flinkpb.FlinkOperator_UDTF {
		return true
	}
	return false
}

func subQueryTable(ssql SqlStack, as string) (sql SqlStack, err error) {
	var notFirst bool

	if ssql.Standard == true {
		err = fmt.Errorf("can't use Standard sql, subquery alias name is " + as)
		return
	}
	sql = ssql
	sql.Distinct = constants.DISTINCT_ALL // actual distinct in subquery
	sql.TableAs = as
	sql.Other = ""
	sql.Column = []flinkpb.ColumnAs{}
	for _, c := range ssql.Column {
		var column flinkpb.ColumnAs
		if c.As != "" {
			column.Field = c.As
		} else {
			column.Field = c.Field
		}
		sql.Column = append(sql.Column, column)
	}
	sql.Table = " ( select " + ssql.Distinct + " "
	notFirst = false
	for _, c := range ssql.Column {
		if notFirst == true {
			sql.Table += ","
		}
		sql.Table += c.Field
		if c.As != "" {
			sql.Table += " as " + c.As
		}
		notFirst = true
	}
	sql.Table += " from " + ssql.Table + " "
	if ssql.TableAs != "" {
		ssql.Table += " as " + ssql.TableAs
	}
	sql.Table += ssql.Other + " )"

	return
}

func appendID(IDs []string, appendIDs []string) (newIDs []string) {
	var (
		found bool
	)

	for _, aid := range appendIDs {
		found = false
		for _, id := range IDs {
			if id == aid {
				found = true
				break
			}
		}
		if found == false {
			IDs = append(IDs, aid)
		}
	}
	newIDs = IDs

	return
}

func printOperator(dag []*flinkpb.FlinkOperator, d flinkpb.FlinkOperator, ssql SqlStack) (sql SqlStack, err error) {
	var (
		notFirst          bool
		upStreamNode      flinkpb.FlinkOperator
		str               string
		upStreamRightNode flinkpb.FlinkOperator
	)

	if d.Type == flinkpb.FlinkOperator_Empty {
		sql = ssql
		return
	} else {
		ssql.NodeCount += 1
	}

	if d.Type == flinkpb.FlinkOperator_Dest {
		v := d.Property.Dest

		str = "insert into " + v.TableId + "("
		notFirst = false
		for _, c := range v.Columns {
			if notFirst == true {
				str += ","
			}
			str += c
			notFirst = true
		}
		str += ") "

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		ssql.TableId = appendID(ssql.TableId, []string{v.TableId})
		sql, err = printOperator(dag, upStreamNode, ssql)
		sql.Sql = str + sql.Sql
		return
	} else if d.Type == flinkpb.FlinkOperator_Values {
		var (
			values string
		)

		v := d.Property.Values

		values = " values"
		firstrow := true
		for _, row := range v.Rows {
			if firstrow == false {
				values += ", "
			}
			firstrow = false

			firstvalue := true
			for _, value := range row.Values {
				if firstvalue == false {
					values += ", "
				} else {
					values += "("
				}
				values += value
				firstvalue = false
			}
			values += ")"
		}

		str += values
		ssql.Sql = str + ssql.Sql
		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Const {
		v := d.Property.Const
		if ssql.Standard == true {
			str += " select "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				str += v.Table + "." + c.As
				notFirst = true
			}
			str += " from ( select "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field + " as " + c.As
				notFirst = true
			}
			str += ") as " + v.Table + " "
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.TableAs = v.Table
			for _, c := range v.Column {
				ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.As})
			}
			ssql.Table = " (select "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				ssql.Table += c.Field + " as " + c.As
				notFirst = true
			}
			ssql.Table += ssql.Other + ")"
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Source {
		var (
			toSubQuery bool
		)

		v := d.Property.Source

		if v.Distinct == constants.DISTINCT_DISTINCT || v.TableAs != "" {
			toSubQuery = true
		}

		if ssql.Standard == true {
			str += " select " + v.Distinct + " "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				if c.Func != "" {
					str += c.Func + "(" + c.Field + ")"
				} else {
					str += c.Field
				}
				if c.As != "" {
					str += " as " + c.As
					toSubQuery = true
				}
				notFirst = true
			}
			for _, c := range v.CustomColumn {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
					toSubQuery = true
				}
				notFirst = true
			}
			str += " from " + v.TableId + " "
			ssql.Sql = str + ssql.Sql
		} else {
			var newColumn []flinkpb.ColumnAs

			for _, c := range v.Column {
				var oneNewColumn flinkpb.ColumnAs
				if c.Func != "" {
					oneNewColumn.Field += c.Func + "(" + c.Field + ")"
				} else {
					oneNewColumn.Field += c.Field
				}
				oneNewColumn.As = c.As
				newColumn = append(newColumn, oneNewColumn)
			}
			ssql.Column = newColumn
			for _, c := range v.CustomColumn {
				ssql.Column = append(ssql.Column, *c)
			}
			ssql.Distinct = v.Distinct
			ssql.Table = v.TableId
		}
		ssql.TableId = appendID(ssql.TableId, []string{v.TableId})

		if toSubQuery == true {
			ssql, err = subQueryTable(ssql, v.TableAs)
			if err != nil {
				return
			}
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Dimension {
		v := d.Property.Dimension
		if v.Distinct == constants.DISTINCT_DISTINCT {
			err = fmt.Errorf("Dimension Not Allow Distinct")
			return
		}

		if ssql.Standard == true {
			str += " select " + v.Distinct + " "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				if c.Func != "" {
					str += c.Func + "(" + c.Field + ")"
				} else {
					str += c.Field
				}
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			for _, c := range v.CustomColumn {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + v.TableId + " FOR SYSTEM_TIME AS OF " + v.TimeColumn.Field + " AS  " + v.TableAs + " "
			ssql.Sql = str + ssql.Sql
		} else {
			var newColumn []flinkpb.ColumnAs

			for _, c := range v.Column {
				var oneNewColumn flinkpb.ColumnAs
				if c.Func != "" {
					oneNewColumn.Field += c.Func + "(" + c.Field + ")"
				} else {
					oneNewColumn.Field += c.Field
				}
				oneNewColumn.As = c.As
				newColumn = append(newColumn, oneNewColumn)
			}
			ssql.Column = newColumn
			for _, c := range v.CustomColumn {
				ssql.Column = append(ssql.Column, *c)
			}
			ssql.Distinct = v.Distinct
			ssql.Table = v.TableId + " FOR SYSTEM_TIME AS OF " + v.TimeColumn.Field + " AS  " + v.TableAs + " "
		}
		ssql.TableId = appendID(ssql.TableId, []string{v.TableId})

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_OrderBy {
		v := d.Property.OrderBy

		notFirst = false
		str = " order by "
		for _, c := range v.Column {
			if notFirst == true {
				str += ","
			}
			if strings.ToUpper(c.Order) != "ASC" && strings.ToUpper(c.Order) != "DESC" {
				err = fmt.Errorf("older by only allow ASC and DESC")
				return
			}
			str += c.Field + " " + c.Order
			notFirst = true
		}
		str += " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Limit {
		v := d.Property.Limit
		str = " limit " + fmt.Sprintf("%d", v.Limit) + " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Offset {
		v := d.Property.Offset
		str = " offset " + fmt.Sprintf("%d", v.Offset) + " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Fetch {
		v := d.Property.Fetch
		str = " fetch first " + fmt.Sprintf("%d", v.Fetch) + " rows only "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Filter {
		v := d.Property.Filter

		if v.In == "" && v.Exists == "" {
			if d.UpstreamRight == "" {
				str = " where " + v.Where + " "
				if ssql.Standard == true {
					ssql.Sql = str + ssql.Sql
				} else {
					ssql.Other = str + ssql.Other
				}
				upStreamNode, _ = getOperatorNode(dag, d.Upstream)
				sql, err = printOperator(dag, upStreamNode, ssql)
			} else {
				var (
					leftsql  SqlStack
					rightsql SqlStack
				)
				err = fmt.Errorf("not allow two upStream Node")
				return

				upStreamNode, _ = getOperatorNode(dag, d.Upstream)
				leftsql, err = printOperator(dag, upStreamNode, SqlStack{Standard: false})
				upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
				rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: false})

				if isSimpleTable(upStreamNode.Type) == false {
					leftsql, err = subQueryTable(leftsql, d.Id)
					if err != nil {
						return
					}
				}
				if isSimpleTable(upStreamRightNode.Type) == false {
					rightsql, err = subQueryTable(rightsql, d.Id+"right")
					if err != nil {
						return
					}
				}
				ssql.TableId = appendID(ssql.TableId, leftsql.TableId)
				ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
				ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
				ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
				ssql.NodeCount += leftsql.NodeCount
				ssql.NodeCount += rightsql.NodeCount

				if leftsql.Other != "" || rightsql.Other != "" {
					err = fmt.Errorf("the SourceNode of filter only allow one SourceNode. error expression  " + leftsql.Other + " " + rightsql.Other)
					return
				}
				if leftsql.Distinct != rightsql.Distinct {
					err = fmt.Errorf("the source node distinct is different")
					return
				}

				if ssql.Standard == true {
					str = " select " + leftsql.Distinct + " "
					notFirst = false
					for _, c := range leftsql.Column {
						if notFirst == true {
							str += ","
						}
						str += c.Field
						if c.As != "" {
							str += " as " + c.As
						}
						notFirst = true
					}
					for _, c := range rightsql.Column {
						if notFirst == true {
							str += ","
						}
						str += c.Field
						if c.As != "" {
							str += " as " + c.As
						}
					}
					str += " from " + leftsql.Table + ", " + rightsql.Table + " "
					str += " where " + v.Where + " "

					ssql.Sql = str + ssql.Sql
					sql = ssql
					return
				} else {
					ssql.Distinct = leftsql.Distinct
					ssql.Table = leftsql.Table + ", " + rightsql.Table + " "
					str += " where " + v.Where + " "
					ssql.Other = str + ssql.Other

					for _, c := range leftsql.Column {
						ssql.Column = append(ssql.Column, c)
					}
					for _, c := range rightsql.Column {
						ssql.Column = append(ssql.Column, c)
					}
					sql = ssql
					return
				}
			}
		} else {
			var rightsql SqlStack

			if d.UpstreamRight != "" {
				upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
				rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: true})
				if err != nil {
					return
				}
				ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
				ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
				ssql.NodeCount += rightsql.NodeCount
				if v.In != "" {
					str = " where " + v.In + " in "
				} else {
					str = " where " + v.Exists + " exists "
				}

				if ssql.Standard == true {
					ssql.Sql = str + "(" + rightsql.Sql + ")" + ssql.Sql
				} else {
					ssql.Other = str + "(" + rightsql.Other + ")" + ssql.Sql
				}
			} else {
				if v.In != "" {
					str = " where " + v.In + " in (" + v.Expression + ")"
				} else {
					str = " where " + v.Exists + " in (" + v.Expression + ")"
				}
				if ssql.Standard == true {
					ssql.Sql = str + ssql.Sql
				} else {
					ssql.Other = str + ssql.Sql
				}
			}

			upStreamNode, _ = getOperatorNode(dag, d.Upstream)
			sql, err = printOperator(dag, upStreamNode, ssql)
			return
		}
	} else if d.Type == flinkpb.FlinkOperator_Union {
		var (
			leftsql  SqlStack
			rightsql SqlStack
			all      string
		)

		v := d.Property.Union

		if v.All == true {
			all = " all "
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		leftsql, err = printOperator(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
		rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableId = appendID(ssql.TableId, leftsql.TableId)
		ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

		if ssql.Standard == true {
			str = " select "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				if c.As != "" {
					str += c.As
				} else {
					str += c.Field
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + leftsql.Table + " " + leftsql.Other + ") "
			str += " union " + all + " (" + rightsql.Sql + ")) "
			ssql.Sql = str + ssql.Sql
			sql = ssql
			return
		} else {
			ssql.Distinct = constants.DISTINCT_ALL
			ssql.Column = []flinkpb.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field
				if c.As != "" {
					ssql.Table += " as " + c.As
				}
				notFirst = true
			}
			ssql.Table += " from " + leftsql.Table + " " + leftsql.Other + ") "
			ssql.Table += " union " + all + " (" + rightsql.Sql + ")) "
			sql = ssql
			return
		}
	} else if d.Type == flinkpb.FlinkOperator_Except {
		var (
			leftsql  SqlStack
			rightsql SqlStack
		)

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		leftsql, err = printOperator(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
		rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableId = appendID(ssql.TableId, leftsql.TableId)
		ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

		if ssql.Standard == true {
			str = " select "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				if c.As != "" {
					str += c.As
				} else {
					str += c.Field
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + leftsql.Table + " " + leftsql.Other + ") "
			str += " except (" + rightsql.Sql + ")) "
			ssql.Sql = str + ssql.Sql
			sql = ssql
			return
		} else {
			ssql.Distinct = constants.DISTINCT_ALL
			ssql.Column = []flinkpb.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field
				if c.As != "" {
					ssql.Table += " as " + c.As
				}
				notFirst = true
			}
			ssql.Table += " from " + leftsql.Table + " " + leftsql.Other + ") "
			ssql.Table += " except (" + rightsql.Sql + ")) "
			sql = ssql
			return
		}
	} else if d.Type == flinkpb.FlinkOperator_Intersect {
		var (
			leftsql  SqlStack
			rightsql SqlStack
		)

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		leftsql, err = printOperator(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
		rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableId = appendID(ssql.TableId, leftsql.TableId)
		ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

		if ssql.Standard == true {
			str = " select "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				if c.As != "" {
					str += c.As
				} else {
					str += c.Field
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + leftsql.Table + " " + leftsql.Other + ") "
			str += " intersect (" + rightsql.Sql + ")) "
			ssql.Sql = str + ssql.Sql
			sql = ssql
			return
		} else {
			ssql.Distinct = constants.DISTINCT_ALL
			ssql.Column = []flinkpb.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, flinkpb.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field
				if c.As != "" {
					ssql.Table += " as " + c.As
				}
				notFirst = true
			}
			ssql.Table += " from " + leftsql.Table + " " + leftsql.Other + ") "
			ssql.Table += " intersect (" + rightsql.Sql + ")) "
			sql = ssql
			return
		}
	} else if d.Type == flinkpb.FlinkOperator_GroupBy {
		v := d.Property.GroupBy

		str = " group by "
		notFirst = false
		for _, c := range v.GroupBy {
			if notFirst == true {
				str += ","
			}
			str += c
			notFirst = true
		}
		str += " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_Having {
		v := d.Property.Having
		str = " having " + v.Having + " "
		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		sql, err = printOperator(dag, upStreamNode, ssql)
		return
	} else if d.Type == flinkpb.FlinkOperator_UDTF {
		v := d.Property.Udtf
		if ssql.Standard == true {
			err = fmt.Errorf(flinkpb.FlinkOperator_UDTF.String() + " can't use in standard sql")
			return
		}
		ssql.Distinct = constants.DISTINCT_ALL
		for _, c := range v.SelectColumn {
			ssql.Column = append(ssql.Column, *c)
		}
		ssql.Table = " LATERAL TABLE(" + Quote + v.UdfId + Quote + "(" + v.Args + ")) AS " + v.TableAs + "("
		ssql.UDFID = appendID(ssql.UDFID, []string{v.UdfId})

		notFirst = false
		for _, c := range v.Column {
			if notFirst == true {
				ssql.Table += ","
			}
			ssql.Table += c.Field
			notFirst = true
		}
		ssql.Table += ")"
		sql = ssql
		return
	} else if d.Type == flinkpb.FlinkOperator_UDTTF {
		v := d.Property.Udttf
		if ssql.Standard == true {
			err = fmt.Errorf(flinkpb.FlinkOperator_UDTTF.String() + " can't use in standard sql")
			return
		}
		ssql.UDFID = appendID(ssql.UDFID, []string{v.UdfId})
		ssql.Distinct = constants.DISTINCT_ALL
		for _, c := range v.Column {
			ssql.Column = append(ssql.Column, *c)
		}
		ssql.Table = " LATERAL TABLE(" + Quote + v.UdfId + Quote + "(" + v.Args + "))"
		sql = ssql
		return
	} else if d.Type == flinkpb.FlinkOperator_Join {
		var (
			leftsql  SqlStack
			rightsql SqlStack
		)

		v := d.Property.Join
		if strings.ToUpper(v.Join) != constants.JOIN && strings.ToUpper(v.Join) != constants.LEFT_JOIN && strings.ToUpper(v.Join) != constants.RIGHT_JOIN && strings.ToUpper(v.Join) != constants.FULL_OUT_JOIN && strings.ToUpper(v.Join) != constants.CROSS_JOIN && strings.ToUpper(v.Join) != constants.INTERVAL_JOIN {
			err = fmt.Errorf("don't support this join " + v.Join)
			return
		}
		if strings.ToUpper(v.Join) == constants.INTERVAL_JOIN {
			v.Join = "Where"
		}

		upStreamNode, _ = getOperatorNode(dag, d.Upstream)
		leftsql, err = printOperator(dag, upStreamNode, SqlStack{Standard: false})
		if strings.ToUpper(v.Join) == constants.CROSS_JOIN {
			var (
				crossProperty        flinkpb.SourceOperator
				generateColumnString string
			)
			upStreamRightNode.Type = flinkpb.FlinkOperator_Source
			firstcolumn := true
			for _, generateColumn := range v.GenerateColumn {
				if firstcolumn == false {
					generateColumnString += ", "
				}
				generateColumnString += generateColumn.Field
				firstcolumn = false
			}
			crossProperty.TableId = "CROSS JOIN UNNEST(" + v.Args + ") AS " + v.TableAs + " (" + generateColumnString + ")"
			crossProperty.Distinct = constants.DISTINCT_ALL
			upStreamRightNode.Property.Source = &crossProperty
			rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: false})
		} else {
			upStreamRightNode, _ = getOperatorNode(dag, d.UpstreamRight)
			rightsql, err = printOperator(dag, upStreamRightNode, SqlStack{Standard: false})
		}

		ssql.TableId = appendID(ssql.TableId, leftsql.TableId)
		ssql.TableId = appendID(ssql.TableId, rightsql.TableId)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

		if isSimpleTable(upStreamNode.Type) == false {
			leftsql, err = subQueryTable(leftsql, v.TableAs)
			if err != nil {
				return
			}
		}

		if isSimpleTable(upStreamRightNode.Type) == false {
			rightsql, err = subQueryTable(rightsql, v.TableAs)
			if err != nil {
				return
			}
		}

		if leftsql.Distinct != rightsql.Distinct {
			err = fmt.Errorf("the source node distinct is different")
			return
		}

		if ssql.Standard == true {
			// select column
			str = " select "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}

			// from table
			str += " from ( select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			for _, c := range rightsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + leftsql.Table
			if leftsql.TableAs != "" {
				str += " as " + leftsql.TableAs
			}
			str += " " + v.Join + " " + rightsql.Table
			if rightsql.TableAs != "" {
				str += " as " + rightsql.TableAs
			}

			if v.Expression != "" {
				str += " ON " + v.Expression
			}
			str += ")"
			ssql.Sql = str + ssql.Sql
			sql = ssql
			return
		} else {
			sql = ssql
			sql.Distinct = constants.DISTINCT_ALL
			for _, c := range v.Column {
				ssql.Column = append(ssql.Column, *c)
			}

			sql.Table = " ( select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					sql.Table += ","
				}
				sql.Table += c.Field
				if c.As != "" {
					sql.Table += " as " + c.As
				}
				notFirst = true
			}
			for _, c := range rightsql.Column {
				if notFirst == true {
					sql.Table += ","
				}
				sql.Table += c.Field
				if c.As != "" {
					sql.Table += " as " + c.As
				}
				notFirst = true
			}
			sql.Table += " from " + leftsql.Table
			if leftsql.TableAs != "" {
				sql.Table += " as " + leftsql.TableAs
			}
			sql.Table += " " + v.Join + " " + rightsql.Table
			if rightsql.TableAs != "" {
				sql.Table += " as " + rightsql.TableAs
			}
			if v.Expression != "" {
				sql.Table += " ON " + v.Expression
			}
			sql.Table += ")"
			return
		}
		/*
			} else if d.OperatorType == constants.JarNode {
				var (
					checkv = regexp.MustCompile(`^[a-zA-Z0-9_/. ]*$`).MatchString
				)

				v := d.Property.Jar

				if checkv(v.JarArgs) == false {
					err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarargs")
					return
				}
				if checkv(v.JarEntry) == false {
					err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarentry")
					return
				}
				sql = ssql
				jarByte, _ := json.Marshal(d.Property.Jar)
				sql.Sql = string(jarByte)
				return
		*/
	} else {
		err = fmt.Errorf("unknow nodeType " + d.Type.String())
		return
	}

	return
}

func validTableName(tableName string, mappingName string) string {
	if mappingName == "" {
		return tableName
	} else {
		return mappingName
	}
}

func CreateRandomString(len int) string {
	var container string
	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	b := bytes.NewBufferString(str)
	length := b.Len()
	bigInt := big.NewInt(int64(length))
	for i := 0; i < len; i++ {
		randomInt, _ := rand.Int(rand.Reader, bigInt)
		container += string(str[randomInt.Int64()])
	}
	return container
}

func parserJobInfo(ctx context.Context, job *request.JobParser, engineClient EngineClient, sourceClient SourceClient, udfClient UdfClient, resourceClient ResourceClient, flinkHome string, hadoopConf string, flinkExecuteJars string) (jobElement response.JobParser, err error) {
	if job.GetJob().GetCode().GetType() == model.StreamJob_Jar {
		if job.Command == constants.JobCommandPreview || job.Command == constants.JobCommandSyntax {
			err = fmt.Errorf("jar mode only support run command")
			return
		}

		var (
			entry          string
			jarParallelism string
			jarName        string
			jarUrl         string
		)

		jar := job.GetJob().GetCode().GetJar()
		// conf
		jobElement.ZeppelinConf = "%sh.conf\n\n"
		jobElement.ZeppelinConf += "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years

		// mainrun
		if jarName, jarUrl, err = resourceClient.GetFileById(ctx, jar.GetResourceId()); err != nil {
			return
		}
		localJarPath := job.GetJob().GetJobId() + "/" + jarName
		jobElement.Resources = &model.JobResources{Jar: jar.GetResourceId(), JobId: job.GetJob().GetJobId()}
		jobElement.ZeppelinMainRun = "%sh\n\n" + "useradd -m " + job.GetJob().GetJobId() + "\nsu - " + job.GetJob().GetJobId() + "\n"
		jobElement.ZeppelinMainRun += "mkdir -p " + job.GetJob().GetJobId() + "\n"
		jobElement.ZeppelinMainRun += fmt.Sprintf("hdfs dfs -get %v %v\n", jarUrl, localJarPath)
		if len(jar.JarEntry) > 0 {
			entry = " -c '" + jar.JarEntry + "' "
		} else {
			entry = " "
		}
		if job.GetJob().GetArgs().GetParallelism() > 0 {
			jarParallelism = " -p " + fmt.Sprintf("%d", job.GetJob().GetArgs().GetParallelism()) + " "
		} else {
			jarParallelism = " "
		}
		jobElement.ZeppelinMainRun += flinkHome + "/bin/flink run -d -m " + FlinkHostQuote + ":" + FlinkPortQuote + jarParallelism + entry + localJarPath + " " + jar.GetJarArgs()
	} else {
		var (
			interpreter         string
			interpreter_mainrun string
			runAsOne            string
			sql                 SqlStack
			sourcetypes         []string
		)

		// interpreter
		if job.GetJob().GetArgs().GetRunAsOne() {
			runAsOne = "runAsOne=true"
		} else {
			runAsOne = "runAsOne=false"
		}
		if job.GetJob().GetCode().GetType() == model.StreamJob_Scala {
			interpreter = "%flink\n\n"
			interpreter_mainrun = "%flink(" + runAsOne + ")\n\n"
		} else if job.GetJob().GetCode().GetType() == model.StreamJob_Python {
			interpreter = "%flink.ipyflink\n\n"
			interpreter_mainrun = "%flink.ipyflink(" + runAsOne + ")\n\n"
		} else if job.GetJob().GetCode().GetType() == model.StreamJob_SQL || job.GetJob().GetCode().GetType() == model.StreamJob_Operator {
			var interpreter_params string

			// Batch/Stream
			if true == true {
				interpreter = "%flink.ssql"
			} else {
				interpreter = "%flink.bsql"
			}

			jobName := job.GetJob().GetJobId()
			if job.Command == constants.JobCommandSyntax {
				jobName = "syx-" + CreateRandomString(16)
			}

			interpreter_params = "jobName=" + jobName
			if job.GetJob().GetArgs().GetParallelism() > 0 {
				interpreter_params += ",parallelism=" + fmt.Sprintf("%d", job.GetJob().GetArgs().GetParallelism())
			}

			if job.GetJob().GetCode().GetType() == model.StreamJob_SQL && job.Command == constants.JobCommandRun {
				interpreter_params += "," + runAsOne
			}
			interpreter_mainrun += interpreter + "(" + interpreter_params + ")\n\n"
			interpreter += "\n\n"
		}

		//conf
		jobElement.ZeppelinConf = "%flink.conf\n\n"
		jobElement.ZeppelinConf += "FLINK_HOME " + flinkHome + "\n"
		//TODO
		//jobElement.ZeppelinConf += "HADOOP_CONF_DIR " + hadoopConf + "\n"
		jobElement.ZeppelinConf += "flink.execution.mode remote\n"
		jobElement.ZeppelinConf += "flink.execution.remote.host " + FlinkHostQuote + "\n"
		jobElement.ZeppelinConf += "flink.execution.remote.port " + FlinkPortQuote + "\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentStreamSql.max 1000000\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentBatchSql.max 1000000\n"

		// operator depend
		if job.GetJob().GetCode().GetType() == model.StreamJob_Operator {
			var (
				firstOperator flinkpb.FlinkOperator
			)

			if len(job.GetJob().GetCode().GetOperators()) == 1 {
				if firstOperator, err = getOperatorNodeByType(job.GetJob().GetCode().GetOperators(), flinkpb.FlinkOperator_Source); err != nil {
					return
				}
			} else {
				if firstOperator, err = getOperatorNodeByType(job.GetJob().GetCode().GetOperators(), flinkpb.FlinkOperator_Dest); err != nil {
					return
				}
			}

			if sql, err = printOperator(job.GetJob().GetCode().GetOperators(), firstOperator, SqlStack{Standard: true}); err != nil {
				return
			}
			if sql.NodeCount != len(job.GetJob().GetCode().GetOperators()) {
				err = fmt.Errorf("find alone node. all node must be in one DAG.")
				return
			}

			// replace UDFname
			for _, udfid := range sql.UDFID {
				var udfName string

				if _, udfName, _, err = udfClient.DescribeUdfManager(ctx, udfid); err != nil {
					return
				}
				sql.Sql = strings.Replace(sql.Sql, Quote+udfid+Quote, udfName, -1)
			}

			//depend
			jobElement.ZeppelinDepends = interpreter

			for _, table := range sql.TableId {
				sourceID, tableName, tableSchema, errTmp := sourceClient.DescribeSourceTable(ctx, table)
				if errTmp != nil {
					err = errTmp
					return
				}
				sql.Sql = strings.Replace(sql.Sql, table, tableName, -1)
				sourceType, sourceUrl, errTmp := sourceClient.DescribeSourceManager(ctx, sourceID)
				if errTmp != nil {
					err = errTmp
					return
				}

				find := false
				for _, saveType := range sourcetypes {
					if saveType == sourceType.String() {
						find = true
						break
					}
				}
				if find == false {
					sourcetypes = append(sourcetypes, sourceType.String())
				}

				jobElement.ZeppelinDepends += "drop table if exists " + tableName + ";\n"
				jobElement.ZeppelinDepends += "create table " + tableName + "\n"

				if sourceType == model.DataSource_MySQL {
					m := sourceUrl.GetMysql()
					t := tableSchema.GetMysql()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, t.TimeColumn)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
					jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "mysql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
					jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
					jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
					jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}

				} else if sourceType == model.DataSource_PostgreSQL {
					m := sourceUrl.GetPostgresql()
					t := tableSchema.GetPostgresql()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, t.TimeColumn)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
					jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "postgresql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
					jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
					jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
					jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}

				} else if sourceType == model.DataSource_ClickHouse {
					m := sourceUrl.GetClickhouse()
					t := tableSchema.GetClickhouse()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, t.TimeColumn)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'clickhouse',\n"
					jobElement.ZeppelinDepends += "'url' = 'jdbc:clickhouse://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
					jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
					jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
					jobElement.ZeppelinDepends += "'format' = 'json',\n"
					jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"

					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}

				} else if sourceType == model.DataSource_Kafka {
					m := sourceUrl.GetKafka()
					t := tableSchema.GetKafka()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, nil)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'kafka',\n"
					jobElement.ZeppelinDepends += "'topic' = '" + t.Topic + "',\n"
					jobElement.ZeppelinDepends += "'properties.bootstrap.servers' = '" + m.KafkaBrokers + "',\n"
					jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ", " + opt.Name + " = " + opt.Value + " \n"
					}

				} else if sourceType == model.DataSource_S3 {
					t := tableSchema.GetS3()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, nil)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'filesystem',\n"
					jobElement.ZeppelinDepends += "'path' = '" + t.Path + "',\n"
					jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}
				} else if sourceType == model.DataSource_HDFS {
					m := sourceUrl.GetHdfs()
					t := tableSchema.GetHdfs()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, nil)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'filesystem',\n"
					jobElement.ZeppelinDepends += "'path' = 'hdfs://" + m.GetNodes().GetNameNode() + ":" + fmt.Sprintf("%d", m.GetNodes().GetPort()) + "/" + t.Path + "',\n"
					jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}
				} else if sourceType == model.DataSource_HBase {
					m := sourceUrl.GetHbase()
					t := tableSchema.GetHbase()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, t.TimeColumn)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'hbase-2.2',\n"
					jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
					jobElement.ZeppelinDepends += "'zookeeper.quorum' = '" + m.Zookeeper + "',\n"
					jobElement.ZeppelinDepends += "'zookeeper.znode.parent' = '" + m.ZNode + "'\n"

					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}
				} else if sourceType == model.DataSource_Ftp {
					m := sourceUrl.GetFtp()
					t := tableSchema.GetFtp()

					jobElement.ZeppelinDepends += "("
					jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn, nil)
					jobElement.ZeppelinDepends += ") WITH (\n"
					jobElement.ZeppelinDepends += "'connector' = 'ftp',\n"
					jobElement.ZeppelinDepends += "'host' = '" + m.Host + "',\n"
					jobElement.ZeppelinDepends += "'port' = '" + fmt.Sprintf("%d", m.Port) + "',\n"
					jobElement.ZeppelinDepends += "'username' = '" + m.Username + "',\n"
					jobElement.ZeppelinDepends += "'password' = '" + m.Password + "',\n"
					jobElement.ZeppelinDepends += "'path' = '" + t.Path + "',\n"
					jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"
					for _, opt := range t.ConnectorOptions {
						jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
					}
				} else {
					err = fmt.Errorf("don't support this source mananger %s", sourceType)
					return
				}

				jobElement.ZeppelinDepends += ");\n\n\n"
			}
		}
		// conf executeJar
		executeJars := ""
		for _, jar := range strings.Split(strings.Replace(flinkExecuteJars, " ", "", -1), ";") {
			sourceType := strings.Split(jar, ":")[0]
			executeJar := strings.Split(jar, ":")[1]

			if job.GetJob().GetCode().GetType() == model.StreamJob_Operator {
				for _, jobSourceType := range sourcetypes {
					if sourceType == jobSourceType {
						if len(executeJars) > 0 {
							executeJars += ","
						}
						executeJars += executeJar
					}
				}
			} else {
				if len(executeJars) > 0 {
					executeJars += ","
				}
				executeJars += executeJar
			}
		}
		jobElement.ZeppelinConf += "flink.execution.jars " + executeJars + "\n"

		// conf.udf
		// ZeppelinPythonUDF
		// ZeppelinScalaUDF
		firstScala := true
		firstJar := true
		firstPython := true
		allUDFs := []string{}
		allUDFs = append(allUDFs, sql.UDFID...)
		allUDFs = append(allUDFs, job.GetJob().GetArgs().GetFunction().GetUdfIds()...)
		allUDFs = append(allUDFs, job.GetJob().GetArgs().GetFunction().GetUdtfIds()...)
		allUDFs = append(allUDFs, job.GetJob().GetArgs().GetFunction().GetUdttfIds()...)
		for _, udfid := range allUDFs {
			var (
				udfLanguage model.UDFInfo_Language
				udfDefine   string
				udfName     string
			)

			if udfLanguage, udfName, udfDefine, err = udfClient.DescribeUdfManager(ctx, udfid); err != nil {
				return
			}

			if udfLanguage == model.UDFInfo_Python {
				if firstPython == true {
					jobElement.ZeppelinPythonUDF = "%flink.ipyflink\n\n"
					firstPython = false
				}
				jobElement.ZeppelinPythonUDF += udfDefine + "\n\n"
				jobElement.ZeppelinPythonUDF = strings.Replace(jobElement.ZeppelinPythonUDF, UDFQuote, udfName, -1)
			} else if udfLanguage == model.UDFInfo_Scala {
				if firstScala == true {
					jobElement.ZeppelinScalaUDF = "%flink\n\n"
					firstScala = false
				}
				jobElement.ZeppelinScalaUDF += udfDefine + "\n\n"
				jobElement.ZeppelinScalaUDF = strings.Replace(jobElement.ZeppelinScalaUDF, UDFQuote, udfName, -1)
			} else if udfLanguage == model.UDFInfo_Java {
				if firstJar == true {
					jobElement.ZeppelinConf += "flink.udf.jars " + udfDefine
					firstJar = false
				} else {
					jobElement.ZeppelinConf += "," + udfDefine
				}
			}
		}
		jobElement.ZeppelinConf += "\n"

		//mainrun
		jobElement.ZeppelinMainRun = interpreter_mainrun
		if job.GetJob().GetCode().GetType() == model.StreamJob_Scala {
			jobElement.ZeppelinMainRun += job.GetJob().GetCode().GetScala().GetCode()
		} else if job.GetJob().GetCode().GetType() == model.StreamJob_Python {
			jobElement.ZeppelinMainRun += job.GetJob().GetCode().GetPython().GetCode()
		} else if job.GetJob().GetCode().GetType() == model.StreamJob_SQL {
			if job.Command == constants.JobCommandSyntax {
				jobElement.ZeppelinMainRun = "%sh\n\n" + SynatxCmd + base64.StdEncoding.EncodeToString([]byte(job.GetJob().GetCode().GetSql().GetCode()))
			} else {
				jobElement.ZeppelinMainRun += job.GetJob().GetCode().GetSql().GetCode()
			}
		} else if job.GetJob().GetCode().GetType() == model.StreamJob_Operator {
			if job.Command == constants.JobCommandSyntax {
				jobElement.ZeppelinMainRun = "%sh\n\n" + SynatxCmd + base64.StdEncoding.EncodeToString([]byte(sql.Sql))
			} else {
				jobElement.ZeppelinMainRun += sql.Sql
			}
		}
	}

	if job.Command != constants.JobCommandSyntax {
		var engine_resp *response.DescribeFlinkClusterAPI
		engine_resp, err = engineClient.client.DescribeFlinkClusterAPI(ctx, &request.DescribeFlinkClusterAPI{SpaceId: job.GetJob().GetSpaceId(), ClusterId: job.GetJob().GetArgs().GetClusterId()})
		if err != nil {
			return
		}

		jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkHostQuote, strings.Split(engine_resp.GetURL(), ":")[0], -1)
		jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkPortQuote, strings.Split(engine_resp.GetURL(), ":")[1], -1) //ip
		jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkHostQuote, strings.Split(engine_resp.GetURL(), ":")[0], -1)
		jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkPortQuote, strings.Split(engine_resp.GetURL(), ":")[1], -1)

		//jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkHostQuote, "flinkjobmanager", -1)
		//jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkPortQuote, "8081", -1) //ip
		//jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkHostQuote, "flinkjobmanager", -1)
		//jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkPortQuote, "8081", -1)
	}

	return
}

func FlinkJobFree(req *request.JobFree) (resp response.JobFree, err error) {
	if req.Resources.Jar != "" {
		ZeppelinDeleteJar := "%sh\n\n"
		ZeppelinDeleteJar += "rm -rf /home/" + req.Resources.JobId + "\n"
		ZeppelinDeleteJar += "deluser " + req.Resources.JobId

		resp.ZeppelinDeleteJar = ZeppelinDeleteJar
	}

	return
}
