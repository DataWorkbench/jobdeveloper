package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/udfpb"
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

type SqlStack struct {
	TableID   []string
	UDFID     []string
	NodeCount int
	Standard  bool
	Sql       string
	Table     string
	Distinct  string
	Column    []constants.ColumnAs
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

func checkDagNodeRelations(dag []constants.DagNode) (err error) {
	for _, d := range dag {
		var (
			relation          NodeRelation
			upStreamNode      constants.DagNode
			upStreamRightNode constants.DagNode
			downStreamNode    constants.DagNode
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

func getDagNode(dag []constants.DagNode, nodeID string) (node constants.DagNode, err error) {
	if nodeID == "" {
		node = constants.DagNode{NodeType: constants.EmptyNode}
	} else {
		var found bool

		for _, d := range dag {
			if d.NodeID == nodeID {
				node = d
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

func getDagNodeByType(dag []constants.DagNode, nodeType string) (node constants.DagNode, err error) {
	var found bool

	for _, d := range dag {
		if d.NodeType == nodeType {
			node = d
			found = true
		}
	}

	if found == false {
		err = fmt.Errorf("can't find the nodeType " + nodeType)
	}

	return
}

func turnToDagNode(dagstr constants.JSONString) (dag []constants.DagNode, err error) {
	err = json.Unmarshal([]byte(dagstr), &dag)
	if err != nil {
		return
	}
	err = checkDagNodeRelations(dag)
	if err != nil {
		return
	}

	return
}

func GetNodeRelation(nodeType string) (nodeRelation NodeRelation, jsonRelation string, err error) {
	var found bool

	if len(nodeRelations) == 0 {
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.UDFNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.UDFNode, constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode},
			AllowDownStream: []string{constants.ValuesNode, constants.SourceNode, constants.FilterNode, constants.ConstNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ValuesNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.UDFNode},
			AllowDownStream: []string{constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.DestNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.ValuesNode, constants.EmptyNode}, // upstream EmptyNode used for sql preview
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ConstNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.UDFNode},
			AllowDownStream: []string{constants.DestNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.SourceNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.UDFNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.UDTFNode, constants.EmptyNode}}) //downstream EmptyNode used for sql preview
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.OrderByNode,
			AllowUpStream:   []string{constants.SourceNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.DestNode, constants.UDFNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.LimitNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.UDFNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.OffsetNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.UDFNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.FetchNode,
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode},
			AllowDownStream: []string{constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.UDFNode, constants.DestNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.FilterNode,
			AllowUpStream:   []string{constants.SourceNode, constants.UDFNode, constants.ConstNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.UDTTFNode},
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
			AllowUpStream:   []string{constants.SourceNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.ConstNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode, constants.UDTFNode, constants.ArraysNode, constants.UDTTFNode},
			AllowDownStream: []string{constants.DestNode, constants.OrderByNode, constants.LimitNode, constants.OffsetNode, constants.FetchNode, constants.JoinNode, constants.UnionNode, constants.ExceptNode, constants.IntersectNode, constants.FilterNode, constants.GroupByNode, constants.HavingNode, constants.WindowNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.UDTTFNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.SourceNode},
			AllowDownStream: []string{constants.JoinNode, constants.FilterNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.ArraysNode,
			AllowUpStream:   []string{constants.EmptyNode, constants.SourceNode},
			AllowDownStream: []string{constants.JoinNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.SqlNode,
			AllowUpStream:   []string{constants.EmptyNode},
			AllowDownStream: []string{constants.EmptyNode}})
		nodeRelations = append(nodeRelations, NodeRelation{
			NodeType:        constants.JarNode,
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
	if NodeType == constants.SourceNode || NodeType == constants.ConstNode || NodeType == constants.UDTTFNode || NodeType == constants.UDTFNode || NodeType == constants.ArraysNode {
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
	sql.Column = []constants.ColumnAs{}
	for _, c := range ssql.Column {
		var column constants.ColumnAs
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

func printNode(dag []constants.DagNode, d constants.DagNode, ssql SqlStack) (sql SqlStack, err error) {
	var (
		notFirst          bool
		upStreamNode      constants.DagNode
		str               string
		upStreamRightNode constants.DagNode
	)

	if d.NodeType == constants.EmptyNode {
		sql = ssql
		return
	} else {
		ssql.NodeCount += 1
	}

	if d.NodeType == constants.DestNode {
		var (
			v constants.DestNodeProperty
		)

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if len(v.Column) == 0 {
			err = fmt.Errorf("please set DestNode column")
			return
		}

		str = "insert into " + v.Table + "("
		notFirst = false
		for _, c := range v.Column {
			if notFirst == true {
				str += ","
			}
			str += c
			notFirst = true
		}
		str += ") "

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		ssql.TableID = appendID(ssql.TableID, []string{v.ID})
		sql, err = printNode(dag, upStreamNode, ssql)
		sql.Sql = str + sql.Sql
		return
	} else if d.NodeType == constants.UDFNode {
		var (
			v constants.UDFNodeProperty
		)

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		ssql.UDFID = appendID(ssql.UDFID, []string{v.ID})
		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.ValuesNode {
		var v constants.ValuesNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}
		str += " values"
		notFirst = false
		for _, r := range v.Row {
			if notFirst == true {
				str += ","
			}
			str += "(" + r + ")"
			notFirst = true
		}
		ssql.Sql = str + ssql.Sql
		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.ConstNode {
		var v constants.ConstNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if ssql.Standard == true {
			str += " select "
			notFirst = false
			for _, c := range v.Column {
				if notFirst == true {
					str += ","
				}
				str += d.NodeID + "." + c.As
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
			str += ") as " + d.NodeID + " "
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.TableAs = d.NodeID
			for _, c := range v.Column {
				ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.As})
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
		var v constants.SourceNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if ssql.Standard == true {
			str += " select " + v.Distinct + " "
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
			str += " from " + v.Table + " "
			ssql.Sql = str + ssql.Sql
		} else {
			ssql.Column = v.Column
			ssql.Distinct = v.Distinct
			ssql.Table = v.Table
		}
		ssql.TableID = appendID(ssql.TableID, []string{v.ID})

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		sql, err = printNode(dag, upStreamNode, ssql)
		return
	} else if d.NodeType == constants.OrderByNode {
		var v constants.OrderByNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.LimitNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.OffsetNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.FetchNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.FilterNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if v.In == "" && v.Exists == "" {
			if d.UpStreamRight == "" {
				str = " where " + v.Filter + " "
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
						str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
						if c.As != "" {
							str += " as " + c.As
						}
						notFirst = true
					}
					for _, c := range rightsql.Column {
						if notFirst == true {
							str += ","
						}
						str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
						if c.As != "" {
							str += " as " + c.As
						}
					}
					str += " from " + leftsql.Table + ", " + rightsql.Table + " "
					str += " where " + v.Filter + " "

					ssql.Sql = str + ssql.Sql
					sql = ssql
					return
				} else {
					ssql.Distinct = leftsql.Distinct
					ssql.Table = leftsql.Table + ", " + rightsql.Table + " "
					str += " where " + v.Filter + " "
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

			if d.UpStreamRight == "" {
				err = fmt.Errorf("in/exist can't find the subquery(UpStreamRight)")
				return
			}

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
			upStreamNode, _ = getDagNode(dag, d.UpStream)
			sql, err = printNode(dag, upStreamNode, ssql)
			return
		}
	} else if d.NodeType == constants.UnionNode {
		var (
			leftsql  SqlStack
			rightsql SqlStack
			v        constants.UnionNodeProperty
		)

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
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
					str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
				if c.As != "" {
					str += " as " + c.As
				}
				notFirst = true
			}
			str += " from " + leftsql.Table + " " + leftsql.Other + ") "
			str += " union " + v.All + " (" + rightsql.Sql + ")) "
			ssql.Sql = str + ssql.Sql
			sql = ssql
			return
		} else {
			ssql.Distinct = constants.DISTINCT_ALL
			ssql.Column = []constants.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
				if c.As != "" {
					ssql.Table += " as " + c.As
				}
				notFirst = true
			}
			ssql.Table += " from " + leftsql.Table + " " + leftsql.Other + ") "
			ssql.Table += " union " + v.All + " (" + rightsql.Sql + ")) "
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
					str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
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
			ssql.Column = []constants.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
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
					str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
				}
				notFirst = true
			}
			str += " from ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					str += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
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
			ssql.Column = []constants.ColumnAs{}
			for _, c := range leftsql.Column {
				if c.As != "" {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.As})
				} else {
					ssql.Column = append(ssql.Column, constants.ColumnAs{Field: c.Field})
				}
			}
			ssql.Table = " ((select " + leftsql.Distinct + " "
			notFirst = false
			for _, c := range leftsql.Column {
				if notFirst == true {
					ssql.Table += ","
				}
				str += c.Field //don't use table.column because the Field maybe not a Column. if we have the same column we can write table.column into Field in SourceNode
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
		var v constants.GroupByNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.HavingNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.WindowNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

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
		var v constants.UDTFNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}
		if ssql.Standard == true {
			err = fmt.Errorf(constants.UDTFNode + " can't use in standard sql")
			return
		}
		ssql.Distinct = constants.DISTINCT_ALL
		ssql.Column = v.SelectColumn
		ssql.Table = " LATERAL TABLE(" + v.FuncName + "(" + v.Args + ")) AS " + v.As + "("
		ssql.UDFID = appendID(ssql.UDFID, []string{v.ID})

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
	} else if d.NodeType == constants.ArraysNode {
		var v constants.ArraysNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}
		if ssql.Standard == true {
			err = fmt.Errorf(constants.ArraysNode + " can't use in standard sql")
			return
		}
		ssql.Distinct = constants.DISTINCT_ALL
		ssql.Column = v.SelectColumn
		ssql.Table = "UNNEST(" + v.Args + ")) AS " + v.As + "("

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
		var v constants.UDTTFNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}
		if ssql.Standard == true {
			err = fmt.Errorf(constants.UDTFNode + " can't use in standard sql")
			return
		}
		ssql.UDFID = appendID(ssql.UDFID, []string{v.ID})
		ssql.Distinct = constants.DISTINCT_ALL
		ssql.Column = v.Column
		ssql.Table = " LATERAL TABLE(" + v.FuncName + "(" + v.Args + "))"
		sql = ssql
		return
	} else if d.NodeType == constants.JoinNode {
		var (
			leftsql  SqlStack
			rightsql SqlStack
			v        constants.JoinNodeProperty
		)

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if strings.ToUpper(v.Join) != constants.JOIN && strings.ToUpper(v.Join) != constants.LEFT_JOIN && strings.ToUpper(v.Join) != constants.RIGHT_JOIN && strings.ToUpper(v.Join) != constants.FULL_OUT_JOIN && strings.ToUpper(v.Join) != constants.CROSS_JOIN {
			err = fmt.Errorf("don't support this join " + v.Join)
			return
		}

		upStreamNode, _ = getDagNode(dag, d.UpStream)
		leftsql, err = printNode(dag, upStreamNode, SqlStack{Standard: false})
		upStreamRightNode, _ = getDagNode(dag, d.UpStreamRight)
		rightsql, err = printNode(dag, upStreamRightNode, SqlStack{Standard: false})

		ssql.TableID = appendID(ssql.TableID, leftsql.TableID)
		ssql.TableID = appendID(ssql.TableID, rightsql.TableID)
		ssql.UDFID = appendID(ssql.UDFID, leftsql.UDFID)
		ssql.UDFID = appendID(ssql.UDFID, rightsql.UDFID)
		ssql.NodeCount += leftsql.NodeCount
		ssql.NodeCount += rightsql.NodeCount

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

		if (strings.ToUpper(v.Join) == constants.CROSS_JOIN && upStreamNode.NodeType != constants.ArraysNode && upStreamRightNode.NodeType != constants.ArraysNode) ||
			((upStreamNode.NodeType == constants.ArraysNode || upStreamRightNode.NodeType == constants.ArraysNode) && strings.ToUpper(v.Join) != constants.CROSS_JOIN) {
			err = fmt.Errorf("cross join must use with ArraysNode")
			return
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
			sql.Column = v.Column

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
		var v constants.SqlNodeProperty

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		sql = ssql
		sql.Sql = v.Sql

		return
	} else if d.NodeType == constants.JarNode {
		var (
			checkv = regexp.MustCompile(`^[a-zA-Z0-9_/. ]*$`).MatchString
			v      constants.JarNodeProperty
		)

		err = json.Unmarshal([]byte(d.Property), &v)
		if err != nil {
			return
		}

		if checkv(v.JarArgs) == false {
			err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarargs")
			return
		}
		if checkv(v.JarEntry) == false {
			err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarentry")
			return
		}
		sql = ssql
		sql.Sql = string(d.Property)
		return

	} else {
		err = fmt.Errorf("unknow nodeType " + d.NodeType)
		return
	}

	return
}

func printSqlAndElement(ctx context.Context, dag []constants.DagNode, job constants.FlinkNode, sourceClient SourceClient, udfClient UdfClient, fileClient FileClient, flinkHome string, flinkExecuteJars string, spaceID string, engineID string, jobID string, command string) (jobElement constants.JobElementFlink, err error) {
	const (
		sqlMode = iota + 1
		jarMode
		operatorMode
	)
	var (
		d              constants.DagNode
		sql            SqlStack
		mode           int32
		engineRequest  constants.EngineRequestOptions
		engineResponse constants.EngineResponseOptions
		jobenv         constants.StreamFlowEnv
		hdfsJarPath    string
		jarName        string
	)

	mode = operatorMode
	if dag[0].NodeType == constants.SqlNode {
		if len(dag) != 1 {
			err = fmt.Errorf("only support one SqlNode")
		} else {
			d, err = getDagNodeByType(dag, constants.SqlNode)
		}
		mode = sqlMode
	} else if dag[0].NodeType == constants.SourceNode && len(dag) == 1 {
		d, err = getDagNodeByType(dag, constants.SourceNode)
	} else if dag[0].NodeType == constants.JarNode {
		if len(dag) != 1 {
			err = fmt.Errorf("only support one JarNode")
		} else {
			d, err = getDagNodeByType(dag, constants.JarNode)
		}
		mode = jarMode
	} else {
		d, err = getDagNodeByType(dag, constants.DestNode)
	}
	if err != nil {
		return
	}
	sql, err = printNode(dag, d, SqlStack{Standard: true})

	if sql.NodeCount != len(dag) {
		err = fmt.Errorf("find alone node. all node must be in one DAG.")
		return
	}

	err = json.Unmarshal([]byte(job.Env), &jobenv)
	if err != nil {
		return
	}

	engineRequest.JobID = jobID
	engineRequest.EngineID = engineID
	engineRequest.WorkspaceID = spaceID
	engineRequest.Parallelism = jobenv.Parallelism
	engineRequest.JobCU = jobenv.JobCU
	engineRequest.TaskCU = jobenv.TaskCU
	engineRequest.TaskNum = jobenv.TaskNum
	if mode == jarMode {
		if command == constants.PreviewCommand || command == constants.SyntaxCheckCommand {
			err = fmt.Errorf("jar mode only support run/explain command")
			return
		}
		var (
			jar            constants.JarNodeProperty
			entry          string
			jarParallelism string
			localJarPath   string
			localJarDir    string
		)
		// conf
		jobElement.ZeppelinConf = "%sh.conf\n\n"
		jobElement.ZeppelinConf += "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years

		err = json.Unmarshal([]byte(sql.Sql), &jar)
		if err != nil {
			return
		}

		jobElement.ZeppelinMainRun = "%sh\n\n"
		if jarName, hdfsJarPath, err = fileClient.GetFileById(ctx, jar.JarPath); err != nil {
			return
		}
		localJarDir = "/tmp/" + jobID
		localJarPath = localJarDir + "/" + jarName
		jobElement.ZeppelinMainRun += "mkdir -p " + localJarDir + "\n"

		jobElement.ZeppelinMainRun += fmt.Sprintf("hdfs dfs -get %v %v\n", hdfsJarPath, localJarPath)
		jobElement.Resources.Jar = localJarDir
		if len(jar.JarEntry) > 0 {
			entry = " -c '" + jar.JarEntry + "' "
		} else {
			entry = ""
		}

		if jobenv.Parallelism > 0 {
			jarParallelism = " -p " + fmt.Sprintf("%d", jobenv.Parallelism) + " "
		} else {
			jarParallelism = " "
		}
		jobElement.ZeppelinMainRun += flinkHome + "/bin/flink run -sae -m " + FlinkHostQuote + ":" + FlinkPortQuote + jarParallelism + entry + localJarPath + " " + jar.JarArgs
		engineRequest.AccessKey = jar.AccessKey
		engineRequest.SecretKey = jar.SecretKey
		engineRequest.EndPoint = jar.EndPoint
		engineRequest.HbaseHosts = jar.HbaseHosts
	} else {
		var (
			title       string
			sourcetypes []string
			executeJars string
			udfList     udfpb.ListsRequest
			udfListRep  *udfpb.ListsReply
		)
		// conf
		jobElement.ZeppelinConf = "%flink.conf\n\n"
		jobElement.ZeppelinConf += "FLINK_HOME " + flinkHome + "\n"
		jobElement.ZeppelinConf += "flink.execution.mode remote\n"
		jobElement.ZeppelinConf += "flink.execution.remote.host " + FlinkHostQuote + "\n"
		jobElement.ZeppelinConf += "flink.execution.remote.port " + FlinkPortQuote + "\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentStreamSql.max 1000000\n"
		jobElement.ZeppelinConf += "zeppelin.flink.concurrentBatchSql.max 1000000\n"

		// title
		if job.StreamSql == true {
			title = "%flink.ssql"
		} else {
			title = "%flink.bsql"
		}
		if jobenv.Parallelism > 0 {
			title += "(parallelism=" + fmt.Sprintf("%d", jobenv.Parallelism) + ")"
		}
		title += "\n\n"

		//depend
		jobElement.ZeppelinDepends = title

		for _, table := range sql.TableID {
			sourceID, tableName, tableUrl, errTmp := sourceClient.DescribeSourceTable(ctx, table)
			if errTmp != nil {
				err = errTmp
				return
			}
			sourceType, ManagerUrl, errTmp := sourceClient.DescribeSourceManager(ctx, sourceID)
			if errTmp != nil {
				err = errTmp
				return
			}

			if sourceType == constants.SourceTypeS3 {
				var m constants.SourceS3Params

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if jobElement.S3info.AccessKey == "" {
					jobElement.S3info.AccessKey = m.AccessKey
					jobElement.S3info.SecretKey = m.SecretKey
					jobElement.S3info.EndPoint = m.EndPoint
				} else if jobElement.S3info.AccessKey != m.AccessKey || jobElement.S3info.SecretKey != m.SecretKey || jobElement.S3info.EndPoint != m.EndPoint {
					err = fmt.Errorf("only allow one s3 sourcemanger in a job, all accesskey secretkey endpoint is the same")
					return
				}
			}

			if sourceType == constants.SourceTypeHbase {
				var v constants.SourceHbaseParams

				if err = json.Unmarshal([]byte(ManagerUrl), &v); err != nil {
					return
				}
				if jobElement.HbaseHosts != "" {
					jobElement.HbaseHosts += ";"
				}
				jobElement.HbaseHosts += v.Hosts
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
				var m constants.SourceMysqlParams
				var t constants.FlinkTableDefineMysql

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "mysql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}

			} else if sourceType == constants.SourceTypePostgreSQL {
				var m constants.SourcePostgreSQLParams
				var t constants.FlinkTableDefinePostgreSQL

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobElement.ZeppelinDepends += "'url' = 'jdbc:" + "postgresql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}

			} else if sourceType == constants.SourceTypeClickHouse {
				var m constants.SourceClickHouseParams
				var t constants.FlinkTableDefineClickHouse

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"
				jobElement.ZeppelinDepends += "'connector' = 'clickhouse',\n"
				jobElement.ZeppelinDepends += "'url' = 'clickhouse://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobElement.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobElement.ZeppelinDepends += "'database-name' = '" + m.Database + "',\n"
				jobElement.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}

			} else if sourceType == constants.SourceTypeKafka {
				var m constants.SourceKafkaParams
				var t constants.FlinkTableDefineKafka

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"

				jobElement.ZeppelinDepends += "'connector' = 'kafka',\n"
				jobElement.ZeppelinDepends += "'topic' = '" + t.Topic + "',\n"
				jobElement.ZeppelinDepends += "'properties.bootstrap.servers' = '" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}

			} else if sourceType == constants.SourceTypeS3 {
				var m constants.SourceS3Params
				var t constants.FlinkTableDefineS3

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"

				jobElement.ZeppelinDepends += "'connector' = 'filesystem',\n"
				jobElement.ZeppelinDepends += "'path' = '" + t.Path + "',\n"
				jobElement.ZeppelinDepends += "'format' = '" + t.Format + "'\n"
				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
				}
			} else if sourceType == constants.SourceTypeHbase {
				var m constants.SourceHbaseParams
				var t constants.FlinkTableDefineHbase

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobElement.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobElement.ZeppelinDepends += column
						first = false
					} else {
						jobElement.ZeppelinDepends += "," + column
					}
				}
				jobElement.ZeppelinDepends += ") WITH (\n"

				jobElement.ZeppelinDepends += "'connector' = 'hbase-2.2',\n"
				jobElement.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobElement.ZeppelinDepends += "'zookeeper.quorum' = '" + m.Zookeeper + "',\n"
				jobElement.ZeppelinDepends += "'zookeeper.znode.parent' = '" + m.Znode + "'\n"

				for _, opt := range t.ConnectorOptions {
					jobElement.ZeppelinDepends += "," + opt + "\n"
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

			if mode == sqlMode {
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
		if command == constants.ExplainCommand || command == constants.SyntaxCheckCommand {
			jobElement.ZeppelinMainRun += "explain "
		}
		jobElement.ZeppelinMainRun += sql.Sql

		udfList.Limit = 100000
		udfList.Offset = 0
		udfList.SpaceID = spaceID
		udfListRep, err = udfClient.client.List(ctx, &udfList)
		if err != nil {
			return
		}

		firstScala := true
		firstJar := true
		for _, udfInfo := range udfListRep.Infos {
			if udfInfo.UdfType == constants.UdfTypePython {
				err = fmt.Errorf("don't support python udf")
				return
			} else if udfInfo.UdfType == constants.UdfTypeScala {
				if firstScala == true {
					jobElement.ZeppelinFuncScala = "%flink\n\n"
					firstScala = false
				}
				jobElement.ZeppelinFuncScala += udfInfo.Define + "\n\n"
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

		engineRequest.AccessKey = jobElement.S3info.AccessKey
		engineRequest.SecretKey = jobElement.S3info.SecretKey
		engineRequest.EndPoint = jobElement.S3info.EndPoint
		engineRequest.HbaseHosts = jobElement.HbaseHosts
	}

	engineRequestByte, _ := json.Marshal(engineRequest)
	engineResponseString, tmperr := GetEngine(string(engineRequestByte))
	if tmperr != nil {
		err = tmperr
		return
	}
	jobElement.Resources.JobID = jobID
	jobElement.Resources.EngineID = engineID

	if err = json.Unmarshal([]byte(engineResponseString), &engineResponse); err != nil {
		return
	}

	jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkHostQuote, engineResponse.EngineHost, -1)
	jobElement.ZeppelinConf = strings.Replace(jobElement.ZeppelinConf, FlinkPortQuote, engineResponse.EnginePort, -1)
	jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkHostQuote, engineResponse.EngineHost, -1)
	jobElement.ZeppelinMainRun = strings.Replace(jobElement.ZeppelinMainRun, FlinkPortQuote, engineResponse.EnginePort, -1)

	return
}

func FlinkJobFree(jobResources string) (freeResources string, err error) {
	var (
		v constants.JobResources
		r constants.JobFreeActionFlink
	)

	err = json.Unmarshal([]byte(jobResources), &v)
	if err != nil {
		return
	}

	if v.JobID != "" {
		//TODO
		FreeEngine(v.JobID)
	}

	if v.Jar != "" {
		r.ZeppelinDeleteJar = "%sh\n\n"
		r.ZeppelinDeleteJar += "rm -rf " + v.Jar

		freeResourcesByte, _ := json.Marshal(r)
		freeResources = string(freeResourcesByte)
	}

	return
}
