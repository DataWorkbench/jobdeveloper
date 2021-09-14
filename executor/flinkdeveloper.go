package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
)

type NodeRelation struct {
	NodeType        string   `json:"nodetype"`
	AllowUpStream   []string `json:"allowupstream"`
	AllowDownStream []string `json:"allowdownstream"`
}

var (
	nodeRelations  []NodeRelation
	Quote          = "$qc$"
	FlinkHostQuote = Quote + "FLINK_HOST" + Quote
	FlinkPortQuote = Quote + "FLINK_PORT" + Quote
)

func GetSqlColumnDefine(sqlColumns []*model.SqlColumnType) (define string) {
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
	if primaryKey != "" {
		define += ", PRIMARY KEY (" + primaryKey + ") NOT ENFORCED"
	}
	return
}

type SqlStack struct {
	TableID   []string
	UDFID     []string
	NodeCount int
	Standard  bool
	Sql       string
	Table     string
	Distinct  string
	Column    []model.ColumnAs
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

func checkDagNodeRelations(dag []*model.FlinkDagNode) (err error) {
	for _, d := range dag {
		var (
			relation          NodeRelation
			upStreamNode      model.FlinkDagNode
			upStreamRightNode model.FlinkDagNode
			downStreamNode    model.FlinkDagNode
		)

		relation, _, err = GetNodeRelation(d.NodeType)
		if err != nil {
			return
		}
		upStreamNode, err = getDagNode(dag, d.UpStream)
		if err != nil {
			return
		}
		downStreamNode, err = getDagNode(dag, d.DownStream)
		if err != nil {
			return
		}

		if In(relation.AllowUpStream, upStreamNode.NodeType) == false {
			err = fmt.Errorf("this node type " + d.NodeType + " only allow these upstream " + strings.Join(relation.AllowUpStream, ",") + " not allow " + upStreamNode.NodeType)
		} else if In(relation.AllowDownStream, downStreamNode.NodeType) == false {
			err = fmt.Errorf("this node type " + d.NodeType + " only allow these downstream " + strings.Join(relation.AllowDownStream, ",") + " not allow " + downStreamNode.NodeType)
		} else {
			if d.NodeType == constants.JoinNode || d.NodeType == constants.UnionNode || d.NodeType == constants.ExceptNode || d.NodeType == constants.IntersectNode {
				upStreamRightNode, err = getDagNode(dag, d.UpStreamRight)
				if err != nil {
					return
				}

				if In(relation.AllowUpStream, upStreamRightNode.NodeType) == false {
					err = fmt.Errorf("this node type " + d.NodeType + " only allow these upstream " + strings.Join(relation.AllowUpStream, ",") + " not allow " + upStreamRightNode.NodeType)
				}
			}
		}
		if err != nil {
			return
		}

	}
	return
}

func getDagNode(dag []*model.FlinkDagNode, nodeID string) (node model.FlinkDagNode, err error) {
	if nodeID == "" {
		node = model.FlinkDagNode{NodeType: constants.EmptyNode}
	} else {
		var found bool

		for _, d := range dag {
			if d.NodeID == nodeID {
				node = *d
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

func getDagNodeByType(dag []*model.FlinkDagNode, nodeType string) (node model.FlinkDagNode, err error) {
	var found bool

	for _, d := range dag {
		if d.NodeType == nodeType {
			node = *d
			found = true
		}
	}

	if found == false {
		err = fmt.Errorf("can't find the nodeType " + nodeType)
	}

	return
}

func GetNodeRelation(nodeType string) (nodeRelation NodeRelation, jsonRelation string, err error) {
	var found bool

	if len(nodeRelations) == 0 {
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ValuesNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.DestNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.ValuesNode, constants.EmptyNode}, // upstream EmptyNode used for sql preview
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ConstNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.DestNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.DimensionNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.DestNode, constants.JoinNode}}) //downstream EmptyNode used for sql preview
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.SourceNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.UDTFNode, constants.EmptyNode}}) //downstream EmptyNode used for sql preview
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.OrderByNode,
			AllowUpStream:   []string{constants.SourceNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.LimitNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.OffsetNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.FetchNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.FilterNode,
			AllowUpStream:   []string{constants.SourceNode, constants.ConstNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.UDTTFNode},
			AllowDownStream: []string{constants.DestNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.UnionNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ExceptNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.IntersectNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.GroupByNode,
			AllowUpStream:   []string{constants.SourceNode, constants.FilterNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.HavingNode,
			AllowUpStream:   []string{constants.SourceNode, constants.FilterNode, constants.GroupByNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.WindowNode,
			AllowUpStream:   []string{constants.SourceNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.UDTFNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.SourceNode},
			AllowDownStream: []string{constants.JoinNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.JoinNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.UDTFNode, constants.UDTTFNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.UDTTFNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.SourceNode},
			AllowDownStream: []string{constants.JoinNode, constants.FilterNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.SqlNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.JarNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ScalaNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.PythonNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.EmptyNode}})
	}

	if nodeType == constants.EmptyNode {
		jsonByte, errtmp := json.Marshal(nodeRelations)
		if errtmp != nil {
			err = errtmp
			return
		}
		jsonRelation = string(jsonByte)
		found = true
	} else {
		for _, n := range nodeRelations {
			if n.NodeType == nodeType {
				nodeRelation = n
				found = true
				break
			}
		}
	}

	if found == false {
		err = fmt.Errorf("can't find this nodeType " + nodeType)
	}

	return
}

func isSimpleTable(NodeType string) bool {
	if NodeType == constants.SourceNode || NodeType == constants.DimensionNode || NodeType == constants.ConstNode || NodeType == constants.UDTTFNode || NodeType == constants.UDTFNode {
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
	sql.Column = []model.ColumnAs{}
	for _, c := range ssql.Column {
		var column model.ColumnAs
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

func printNode(dag []*model.FlinkDagNode, d model.FlinkDagNode, ssql SqlStack) (sql SqlStack, err error) {
	var (
		notFirst          bool
		upStreamNode      model.FlinkDagNode
		str               string
		upStreamRightNode model.FlinkDagNode
	)

	if d.NodeType == constants.EmptyNode {
		sql = ssql
		return
	} else {
		ssql.NodeCount += 1
	}

	if d.NodeType == constants.DestNode {
		v := d.Property.Dest

		str = "insert into " + v.TableID + "("
		notFirst = false
		for _, c := range v.Columns {
			if notFirst == true {
				str += ","
			}
			str += c
			notFirst = true
		}
		str += ") "

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		ssql.TableID = appendID(ssql.TableID, []string{v.TableID})
		sql, err = printNode(dag, upStreamNode, ssql)
		sql.Sql = str + sql.Sql
		return
	} else if d.NodeType == constants.ValuesNode {
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
		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.ConstNode {
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
				ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.As})
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

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.SourceNode {
		var (
			toSubQuery bool
		)

		v := d.Property.Source

		if v.Distinct == constants.DISTINCT_DISTINCT || v.TableAS != "" {
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
				if c.WindowsName != "" {
					str += " over " + c.WindowsName
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
			str += " from " + v.TableID + " "
			ssql.Sql = str + ssql.Sql
		} else {
			var newColumn []model.ColumnAs

			for _, c := range v.Column {
				var oneNewColumn model.ColumnAs
				if c.Func != "" {
					oneNewColumn.Field += c.Func + "(" + c.Field + ")"
				} else {
					oneNewColumn.Field += c.Field
				}
				if c.WindowsName != "" {
					oneNewColumn.Field += " over " + c.WindowsName
				}
				oneNewColumn.As = c.As
				newColumn = append(newColumn, oneNewColumn)
			}
			ssql.Column = newColumn
			for _, c := range v.CustomColumn {
				ssql.Column = append(ssql.Column, *c)
			}
			ssql.Distinct = v.Distinct
			ssql.Table = v.TableID
		}
		ssql.TableID = appendID(ssql.TableID, []string{v.TableID})

		if toSubQuery == true {
			ssql, err = subQueryTable(ssql, v.TableAS)
			if err != nil {
				return
			}
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.DimensionNode {
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
				if c.WindowsName != "" {
					str += " over " + c.WindowsName
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
			str += " from " + v.TableID + " FOR SYSTEM_TIME AS OF " + v.TimeColumn[0].Field + " AS  " + v.TableAS + " "
			ssql.Sql = str + ssql.Sql
		} else {
			var newColumn []model.ColumnAs

			for _, c := range v.Column {
				var oneNewColumn model.ColumnAs
				if c.Func != "" {
					oneNewColumn.Field += c.Func + "(" + c.Field + ")"
				} else {
					oneNewColumn.Field += c.Field
				}
				if c.WindowsName != "" {
					oneNewColumn.Field += " over " + c.WindowsName
				}
				oneNewColumn.As = c.As
				newColumn = append(newColumn, oneNewColumn)
			}
			ssql.Column = newColumn
			for _, c := range v.CustomColumn {
				ssql.Column = append(ssql.Column, *c)
			}
			ssql.Distinct = v.Distinct
			ssql.Table = v.TableID + " FOR SYSTEM_TIME AS OF " + v.TimeColumn[0].Field + " AS  " + v.TableAS + " "
		}
		ssql.TableID = appendID(ssql.TableID, []string{v.TableID})

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.OrderByNode {
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

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.LimitNode {
		v := d.Property.Limit
		str = " limit " + fmt.Sprintf("%d", v.Limit) + " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.OffsetNode {
		v := d.Property.Offset
		str = " offset " + fmt.Sprintf("%d", v.Offset) + " "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.FetchNode {
		v := d.Property.Fetch
		str = " fetch first " + fmt.Sprintf("%d", v.Fetch) + " rows only "

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.FilterNode {
		v := d.Property.Filter

		if v.In == "" && v.Exists == "" {
			if d.UpStreamRight == "" {
				str = " where " + v.Where + " "
				if ssql.Standard == true {
					ssql.Sql = str + ssql.Sql
				} else {
					ssql.Other = str + ssql.Other
				}
				upStreamNode, _ = getDagNode(dag, d.UpStream)
				sql, err = printNode(dag, upStreamNode, ssql)
			} else {
				var (
					leftsql  SqlStack
					rightsql SqlStack
				)
				err = fmt.Errorf("not allow two upStream Node")
				return

				upStreamNode, _ = getDagNode(dag, d.UpStream)
				leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
				upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
				rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: false})

				if isSimpleTable(upStreamNode.NodeType) == false {
					leftsql, err = subQueryTable(leftsql, d.NodeID)
					if err != nil {
						return
					}
				}
				if isSimpleTable(upStreamRightNode.NodeType) == false {
					rightsql, err = subQueryTable(rightsql, d.NodeID+"right")
					if err != nil {
						return
					}
				}
				ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
				ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
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

			if d.UpStreamRight != "" {
				upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
				rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: true})
				if err != nil {
					return
				}
				ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
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
					str = " where " + v.In
				} else {
					str = " where " + v.Exists
				}
				if ssql.Standard == true {
					ssql.Sql = str + ssql.Sql
				} else {
					ssql.Other = str + ssql.Sql
				}
			}

			upStreamNode, _ = getDagNode(dag, d.UpStream)
			sql, err = printNode(dag, upStreamNode, ssql)
			return
		}
	} else if d.NodeType == constants.UnionNode {
		var (
			leftsql  SqlStack
			rightsql SqlStack
			all      string
		)

		v := d.Property.Union

		if v.All == true {
			all = " all "
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
		rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
		ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
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
			ssql.Column = []model.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.Field})
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
	} else if d.NodeType == constants.ExceptNode {
		var (
			leftsql  SqlStack
			rightsql SqlStack
		)

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
		rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
		ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
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
			ssql.Column = []model.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.Field})
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
	} else if d.NodeType == constants.IntersectNode {
		var (
			leftsql  SqlStack
			rightsql SqlStack
		)

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
		rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: true})

		ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
		ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
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
			ssql.Column = []model.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, model.ColumnAs{Field: c.Field})
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
	} else if d.NodeType == constants.GroupByNode {
		v := d.Property.GroupBy

		str = " group by "
		notFirst = false
		for _, c := range v.Groupby {
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

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.HavingNode {
		v := d.Property.Having
		str = " having " + v.Having + " "
		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.WindowNode {
		v := d.Property.Window
		str = " window "
		notFirst = false
		for _, c := range v.Window {
			if notFirst == true {
				str += ","
			}
			str += " " + c.Name + " as (" + c.Spec + ") "
			notFirst = true
		}

		if ssql.Standard == true {
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Other = str + ssql.Other
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.UDTFNode {
		v := d.Property.UDTF
		if ssql.Standard == true {
			err = fmt.Errorf(constants.UDTFNode + " can't use in standard sql")
			return
		}
		ssql.Distinct = constants.DISTINCT_ALL
		for _, c := range v.SelectColumn {
			ssql.Column = append(ssql.Column, *c)
		}
		ssql.Table = " LATERAL TABLE(" + v.UDFID + "(" + v.Args + ")) AS " + v.TableAS + "("
		ssql.UDFID = appendID(ssql.UDFID, []string{v.UDFID})

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
	} else if d.NodeType == constants.UDTTFNode {
		v := d.Property.UDTTF
		if ssql.Standard == true {
			err = fmt.Errorf(constants.UDTTFNode + " can't use in standard sql")
			return
		}
		ssql.UDFID = appendID(ssql.UDFID, []string{v.UDFID})
		ssql.Distinct = constants.DISTINCT_ALL
		for _, c := range v.Column {
			ssql.Column = append(ssql.Column, *c)
		}
		ssql.Table = " LATERAL TABLE(" + v.FuncName + "(" + v.Args + "))"
		sql = ssql
		return
	} else if d.NodeType == constants.JoinNode {
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

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
		if strings.ToUpper(v.Join) == constants.CROSS_JOIN {
			var (
				crossProperty        model.SourceNodeProperty
				generateColumnString string
			)
			upStreamRightNode.NodeType = constants.SourceNode
			firstcolumn := true
			for _, generateColumn := range v.GenerateColumn {
				if firstcolumn == false {
					generateColumnString += ", "
				}
				generateColumnString += generateColumn.Field
				firstcolumn = false
			}
			crossProperty.TableID = "CROSS JOIN UNNEST(" + v.Args + ") AS " + v.TableAS + " (" + generateColumnString + ")"
			crossProperty.Distinct = constants.DISTINCT_ALL
			upStreamRightNode.Property.Source = &crossProperty
			rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: false})
		} else {
			upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
			rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: false})
		}

		ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
		ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

		if isSimpleTable(upStreamNode.NodeType) == false {
			leftsql, err = subQueryTable(leftsql, v.TableAS)
			if err != nil {
				return
			}
		}

		if isSimpleTable(upStreamRightNode.NodeType) == false {
			rightsql, err = subQueryTable(rightsql, v.TableAS)
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
	} else if d.NodeType == constants.SqlNode {
		v := d.Property.Sql

		sql = ssql
		sql.Sql = v.Sql

		return
	} else if d.NodeType == constants.ScalaNode {
		v := d.Property.Scala

		sql = ssql
		sql.Sql = v.Code

		return
	} else if d.NodeType == constants.PythonNode {
		v := d.Property.Python

		sql = ssql
		sql.Sql = v.Code

		return
	} else if d.NodeType == constants.JarNode {
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

	} else {
		err = fmt.Errorf("unknow nodeType " + d.NodeType)
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

func printSqlAndElement(ctx context.Context, dag []*model.FlinkDagNode, job *request.JobParser, engineClient EngineClient, sourceClient SourceClient, udfClient UdfClient, fileClient FileClient, flinkHome string, hadoopConf string, flinkExecuteJars string) (jobElement response.JobParser, err error) {
	const (
		sqlMode = iota + 1
		jarMode
		operatorMode
		scalaMode
		pythonMode
	)

	var (
		d    model.FlinkDagNode
		sql  SqlStack
		mode int32
	)

	if (dag[0].NodeType == constants.SqlNode ||
		dag[0].NodeType == constants.JarNode ||
		dag[0].NodeType == constants.ScalaNode ||
		dag[0].NodeType == constants.PythonNode) && len(dag) != 1 {
		err = fmt.Errorf("only support one SqlNode/JarNode/ScalaNode/PythonNode")
		return
	}

	if dag[0].NodeType == constants.SqlNode {
		d, err = getDagNodeByType(dag, constants.SqlNode)
		mode = sqlMode
	} else if dag[0].NodeType == constants.ScalaNode {
		d, err = getDagNodeByType(dag, constants.ScalaNode)
		mode = scalaMode
	} else if dag[0].NodeType == constants.PythonNode {
		d, err = getDagNodeByType(dag, constants.PythonNode)
		mode = pythonMode
	} else if dag[0].NodeType == constants.SourceNode && len(dag) == 1 {
		d, err = getDagNodeByType(dag, constants.SourceNode)
		mode = operatorMode
	} else if dag[0].NodeType == constants.JarNode {
		d, err = getDagNodeByType(dag, constants.JarNode)
		mode = jarMode
	} else {
		d, err = getDagNodeByType(dag, constants.DestNode)
		mode = operatorMode
	}
	if err != nil {
		return
	}
	sql, err = printNode(dag, d, SqlStack{Standard: true})

	if sql.NodeCount != len(dag) {
		err = fmt.Errorf("find alone node. all node must be in one DAG.")
		return
	}

	jobenv := job.GetJob().GetEnv()
	if mode == jarMode {
		if job.Command == constants.JobCommandPreview || job.Command == constants.JobCommandSyntax {
			err = fmt.Errorf("jar mode only support run/explain command")
			return
		}
		var (
			jar            model.JarNodeProperty
			entry          string
			jarParallelism string
			localJarPath   string
			localJarDir    string
			jarName        string
			url            string
		)
		// conf
		jobElement.ZeppelinConf = "%sh.conf\n\n"
		jobElement.ZeppelinConf += "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years

		err = json.Unmarshal([]byte(sql.Sql), &jar)
		if err != nil {
			return
		}

		jobElement.ZeppelinMainRun = "%sh\n\n" + "useradd -m " + job.Job.JobID + "\nsu - " + job.Job.JobID + "\n"
		if jarName, url, err = fileClient.GetFileById(ctx, jar.JarID); err != nil {
			return
		}
		localJarDir = job.Job.JobID
		localJarPath = localJarDir + "/" + jarName
		jobElement.ZeppelinMainRun += "mkdir -p " + localJarDir + "\n"

		jobElement.ZeppelinMainRun += fmt.Sprintf("hdfs dfs -get %v %v\n", url, localJarPath)
		jobElement.Resources.Jar = localJarDir
		if len(jar.JarEntry) > 0 {
			entry = " -c '" + jar.JarEntry + "' "
		} else {
			entry = ""
		}

		if jobenv.GetFlink().GetParallelism() > 0 {
			jarParallelism = " -p " + fmt.Sprintf("%d", jobenv.GetFlink().GetParallelism()) + " "
		} else {
			jarParallelism = " "
		}
		jobElement.ZeppelinMainRun += flinkHome + "/bin/flink run -sae -m " + FlinkHostQuote + ":" + FlinkPortQuote + jarParallelism + entry + localJarPath + " " + jar.JarArgs
	} else {
		var (
			title       string
			sourcetypes []string
			executeJars string
			udfList     request.ListUDF
			udfListResp *response.ListUDF
		)
		// conf
		jobElement.ZeppelinConf = "%flink.conf\n\n"
		jobElement.ZeppelinConf += "FLINK_HOME " + flinkHome + "\n"
		jobElement.ZeppelinConf += "HADOOP_CONF_DIR " + hadoopConf + "\n"
		jobElement.ZeppelinConf += "flink.execution.mode remote\n"
		jobElement.ZeppelinConf += "flink.execution.remote.host " + FlinkHostQuote + "\n"
		jobElement.ZeppelinConf += "flink.execution.remote.port " + FlinkPortQuote + "\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentStreamSql.max 1000000\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentBatchSql.max 1000000\n"

		// title
		if mode == scalaMode {
			title = "%flink"
		} else if mode == pythonMode {
			title = "%flink.ipyflink"
		} else {
			//if jobenv.StreamSql == true {
			if true == true {
				title = "%flink.ssql"
			} else {
				title = "%flink.bsql"
			}
			if jobenv.GetFlink().GetParallelism() > 0 {
				title += "(parallelism=" + fmt.Sprintf("%d", jobenv.GetFlink().GetParallelism()) + ")"
			}
		}
		title += "\n\n"

		//depend
		jobElement.ZeppelinDepends = title

		for _, udfid := range sql.UDFID {
			_, udfName, _, errTmp := udfClient.DescribeUdfManager(ctx, udfid)
			if errTmp != nil {
				err = errTmp
				return
			}
			sql.Sql = strings.Replace(sql.Sql, udfid, udfName, -1)
		}

		for _, table := range sql.TableID {
			sourceID, tableName, tableUrl, errTmp := sourceClient.DescribeSourceTable(ctx, table)
			if errTmp != nil {
				err = errTmp
				return
			}
			sql.Sql = strings.Replace(sql.Sql, table, tableName, -1)
			sourceType, ManagerUrl, errTmp := sourceClient.DescribeSourceManager(ctx, sourceID)
			if errTmp != nil {
				err = errTmp
				return
			}

			if sourceType == constants.SourceTypeS3 {
				if jobElement.S3.AccessKey == "" {
					jobElement.S3.AccessKey = ManagerUrl.S3.AccessKey
					jobElement.S3.SecretKey = ManagerUrl.S3.SecretKey
					jobElement.S3.Endpoint = ManagerUrl.S3.EndPoint
				} else if jobElement.S3.AccessKey != ManagerUrl.S3.AccessKey || jobElement.S3.SecretKey != ManagerUrl.S3.SecretKey || jobElement.S3.Endpoint != ManagerUrl.S3.EndPoint {
					err = fmt.Errorf("only allow one s3 sourcemanger in a job, all accesskey secretkey endpoint is the same")
					return
				}
			}

			if sourceType == constants.SourceTypeHbase {
				jobElement.Hbase.Hosts = append(jobElement.Hbase.Hosts, ManagerUrl.Hbase.Hosts.Hosts...)
			}

			find := false
			for _, save := range sourcetypes {
				if save == sourceType {
					find = true
					break
				}
			}
			if find == false {
				sourcetypes = append(sourcetypes, sourceType)
			}

			jobElement.ZeppelinDepends += "drop table if exists " + tableName + ";\n"
			jobElement.ZeppelinDepends += "create table " + tableName + "\n"

			if sourceType == constants.SourceTypeMysql {
				m := ManagerUrl.MySQL
				t := tableUrl.MySQL

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "mysql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}

			} else if sourceType == constants.SourceTypePostgreSQL {
				m := ManagerUrl.PostgreSQL
				t := tableUrl.PostgreSQL

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "postgresql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}

			} else if sourceType == constants.SourceTypeClickHouse {
				m := ManagerUrl.ClickHouse
				t := tableUrl.ClickHouse

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'clickhouse',\n"
				jobElement.ZeppelinDepends += "'url' = 'clickhouse://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'database-name' = '" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}

			} else if sourceType == constants.SourceTypeKafka {
				m := ManagerUrl.Kafka
				t := tableUrl.Kafka

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'kafka',\n"
				jobElement.ZeppelinDepends += "'topic' = '" + t.Topic + "',\n"
				jobElement.ZeppelinDepends += "'properties.bootstrap.servers' = '" + m.KafkaBrokers + "',\n"
				jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}

			} else if sourceType == constants.SourceTypeS3 {
				t := tableUrl.S3

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'filesystem',\n"
				jobElement.ZeppelinDepends += "'path' = '" + t.Path + "',\n"
				jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}
			} else if sourceType == constants.SourceTypeHDFS {
				m := ManagerUrl.HDFS
				t := tableUrl.HDFS

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'filesystem',\n"
				jobElement.ZeppelinDepends += "'path' = 'hdfs://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + t.Path + "',\n"
				jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}
			} else if sourceType == constants.SourceTypeHbase {
				m := ManagerUrl.Hbase
				t := tableUrl.Hbase

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'hbase-2.2',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + validTableName(tableName, t.MappingName) + "',\n"
				jobElement.ZeppelinDepends += "'zookeeper.quorum' = '" + m.Zookeeper + "',\n"
				jobElement.ZeppelinDepends += "'zookeeper.znode.parent' = '" + m.Znode + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += ",'" + opt.Name + "' = '" + opt.Value + "'\n"
				}
			} else if sourceType == constants.SourceTypeFtp {
				m := ManagerUrl.Ftp
				t := tableUrl.Ftp

				jobElement.ZeppelinDepends += "("
				jobElement.ZeppelinDepends += GetSqlColumnDefine(t.SqlColumn)
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'ftp',\n"
				jobElement.ZeppelinDepends += "'host' = '" + m.Host + "',\n"
				jobElement.ZeppelinDepends += "'port' = '" + fmt.Sprintf("%d", m.Port) + "',\n"
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

		// conf.executeJar
		for _, jar := range strings.Split(strings.Replace(flinkExecuteJars, " ", "", -1), ";") {
			sourceType := strings.Split(jar, ":")[0]
			executeJar := strings.Split(jar, ":")[1]

			if mode == sqlMode || mode == scalaMode || mode == pythonMode {
				if len(executeJars) > 0 {
					executeJars += ","
				}
				executeJars += executeJar
			} else {
				for _, jobSourceType := range sourcetypes {
					if sourceType == jobSourceType {
						if len(executeJars) > 0 {
							executeJars += ","
						}
						executeJars += executeJar
					}
				}
			}
		}
		jobElement.ZeppelinConf += "flink.execution.jars " + executeJars + "\n"

		//mainrun
		jobElement.ZeppelinMainRun = title
		if job.Command == constants.JobCommandExplain || job.Command == constants.JobCommandSyntax {
			jobElement.ZeppelinMainRun += "explain "
		}
		jobElement.ZeppelinMainRun += sql.Sql

		udfList.Limit = 10000
		udfList.Offset = 0
		udfList.SpaceID = job.Job.SpaceID
		udfListResp, err = udfClient.client.List(ctx, &udfList)
		if err != nil {
			return
		}

		firstScala := true
		firstJar := true
		firstPython := true
		for _, udfInfo := range udfListResp.Infos {
			if udfInfo.UDFType == constants.PythonUDF || udfInfo.UDFType == constants.PythonUDTF {
				if firstPython == true {
					jobElement.ZeppelinPythonUDF = "%flink.ipyflink\n\n"
					firstPython = false
				}
				jobElement.ZeppelinPythonUDF += udfInfo.Define + "\n\n"
			} else if udfInfo.UDFType == constants.ScalaUDF || udfInfo.UDFType == constants.ScalaUDTF || udfInfo.UDFType == constants.ScalaUDTTF {
				if firstScala == true {
					jobElement.ZeppelinScalaUDF = "%flink\n\n"
					firstScala = false
				}
				jobElement.ZeppelinScalaUDF += udfInfo.Define + "\n\n"
			} else {
				if firstJar == true {
					jobElement.ZeppelinConf += "flink.udf.jars " + udfInfo.Define
					firstJar = false
				} else {
					jobElement.ZeppelinConf += "," + udfInfo.Define
				}
			}
		}
		jobElement.ZeppelinConf += "\n"

		if mode == operatorMode {
			job.Job.Env.Hbase = jobElement.Hbase
			job.Job.Env.Flink.S3 = jobElement.S3
		}
	}

	var engineresp *enginepb.CreateFlinkResponse

	engineresp, err = engineClient.client.Create(ctx, &enginepb.CreateFlinkRequest{Name: job.Job.JobID, Namespace: job.Job.SpaceID, WaitingReady: true, WaitingTimeout: 600, Conf: job.Job.Env})
	if err != nil {
		return
	}

	jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkHostQuote, strings.Split(engineresp.Url, ":")[0], -1)
	jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkPortQuote, strings.Split(engineresp.Url, ":")[1], -1) //ip
	jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkHostQuote, strings.Split(engineresp.Url, ":")[0], -1)
	jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkPortQuote, strings.Split(engineresp.Url, ":")[1], -1)

	return
}

func FlinkJobFree(req *request.JobFree) (resp response.JobFree, err error) {
	if req.Resources.Jar != "" {
		ZeppelinDeleteJar := "%sh\n\n"
		ZeppelinDeleteJar += "rm -rf /home/" + req.Resources.JobID + "\n"
		ZeppelinDeleteJar += "deluser " + req.Resources.JobID

		resp.ZeppelinDeleteJar = ZeppelinDeleteJar
	}

	return
}
