// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math/bits"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
)

type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan
}

type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
}

func (s *joinReorderDPSolver) solve(joinGroup []LogicalPlan, eqConds []expression.Expression) (LogicalPlan, error) {
	// The pseudo code can be found in README.
	// And there's some common struct and method like `baseNodeCumCost`, `calcJoinCumCost` you can use in `rule_join_reorder.go`.
	// Also, you can take a look at `rule_join_reorder_greedy.go`, this file implement the join reorder algo based on greedy algorithm.
	// You'll see some common usages in the greedy version.

	// Note that the join tree may be disconnected. i.e. You need to consider the case `select * from t, t1, t2`.
	// 1.对每个joinGroup节点估算其cost,构建jrNode放入curJoinGroup中
	for _, node := range joinGroup {
		_, err := node.recursiveDeriveStats()
		if err != nil {
			return nil, err
		}
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p: 		 node,
			cumCost: s.baseNodeCumCost(node),
		})
	}

	// 2.处理等值条件表达式eqConds
	totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	adjacents := make([][]int, len(s.curJoinGroup))
	for _, cond := range eqConds {
		// 2.1 取出等值条件表达式的leftColumn及其对应节点id
		sf := cond.(*expression.ScalarFunction)
		leftCol := sf.GetArgs()[0].(*expression.Column)
		leftIdx, err := findNodeIndexInGroup(joinGroup, leftCol)
		if err != nil {
			return nil, err
		}
		// 2.2 取出等值条件表达式的rightColumn及其对应节点id
		rightCol := sf.GetArgs()[1].(*expression.Column)
		rightIdx, err := findNodeIndexInGroup(joinGroup, rightCol)
		if err != nil {
			return nil, err
		}
		// 2.3 将EqEdge放入totalEqEdges数组,更新两个节点的adjacents数组(节点间以Edge相连构建图)
		totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
			nodeIDs: []int{leftIdx, rightIdx},
			edge:    sf,
		})
		adjacents[leftIdx] = append(adjacents[leftIdx], rightIdx)
		adjacents[rightIdx] = append(adjacents[rightIdx], leftIdx)
	}

	// 3.处理非等值条件表达式otherConds:
	totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		// 3.1 取出非等值条件表达式对应的Column(可能有多个)
		cols := expression.ExtractColumns(cond)
		// 3.2 在joinGroup中找到这些Column对应的节点,记录下节点id(以bitmap形式)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			colIdx, err := findNodeIndexInGroup(joinGroup, col)
			if err != nil {
				return nil, err
			}
			mask |= 1<<uint(colIdx)
			ids = append(ids, colIdx)
		}
		// 3.3 将NonEqEdge放入totalNonEqEdges数组
		totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
			nodeIDs: 	ids,
			nodeIDMask: mask,
			expr: 		cond,
		})
	}

	// 4.遍历joinGroup,对每个节点执行如下操作,直到求出2.3中构建的整个图的最优Join排序
	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	var joins []LogicalPlan
	for i := 0; i < len(joinGroup); i++ {
		// 4.1 如果该节点已经被访问过了,跳过该节点遍历下一个节点
		if visited[i] {
			continue
		}
		// 4.2 bfs构建该节点的连通子图,visitID2NodeID数组记录了该连通子图的所有节点
		queue := []int{i}
		visited[i] = true
		var visitID2NodeID []int
		for len(queue) > 0 {
			curNodeID := queue[0]
			queue = queue[1:]
			// visited[curNodeID] = true
			nodeID2VisitID[curNodeID] = len(visitID2NodeID)
			visitID2NodeID = append(visitID2NodeID, curNodeID)
			for _, adjNodeID := range adjacents[curNodeID] {
				if visited[adjNodeID] {
					continue
				}
				queue = append(queue, adjNodeID)
				visited[adjNodeID] = true
			}
		}
		// 4.3 nodeIDMask以bitmap的形式记录visitID2NodeID中的节点
		nodeIDMask := uint(0)
		for _, nodeID := range nodeID2VisitID {
			nodeIDMask |= 1<<uint(nodeID)
		}
		// 4.4 从totalNonEqEdges中取出包含在连通子图中的NonEqEdges
		var subNonEqEdges []joinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			if totalNonEqEdges[i].nodeIDMask & nodeIDMask != totalNonEqEdges[i].nodeIDMask {
				continue
			}
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].nodeIDs {
				newMask |= 1<<uint(nodeID2VisitID[nodeID])
			}
			totalNonEqEdges[i].nodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = append(totalNonEqEdges[:i], totalNonEqEdges[i+1:]...)
		}
		// 4.5 dp获取节点连通子图的最优Join排序
		nodeCnt := uint(len(visitID2NodeID))
		bestPlan := make([]*jrNode, 1<<nodeCnt)
		// 4.5.1 初始状态:只包含单个节点的joinGroup的bestPlan就是该节点本身
		for i := uint(0); i < nodeCnt; i++ {
			bestPlan[1<<i] = s.curJoinGroup[visitID2NodeID[i]]
		}
		// 4.5.2 枚举节点位图nodeBitmap(1->1<<nodeCnt)
		for nodeBitmap := uint(1); nodeBitmap < (1<<nodeCnt); nodeBitmap++ {
			if bits.OnesCount(nodeBitmap) == 1 {
				continue
			}
			// 4.5.3 枚举nodeBitmap对应的所有节点组合
			// 如nodeBitmap=11(1011)可能存在(0 join 1) join 3, (0 join 3) join 1, (1 join 3) join 0 这三种情况
			for sub := (nodeBitmap-1) & nodeBitmap; sub > 0; sub = (sub-1) & nodeBitmap {
				remain := nodeBitmap ^ sub
				// 1010 + 0001 和 0001 + 1010 相同,只计算一次
				if remain < sub {
					continue
				}
				// sub或remain包含的节点集合不相连,跳过
				if bestPlan[sub] == nil || bestPlan[remain] == nil {
					continue
				}
				// 将sub和remain相连
				var usedEqEdges []joinGroupEqEdge
				var otherConds  []expression.Expression
				// 取出连接sub和remain的EqEdge: EqEdge的左节点和右节点分别出现在sub和remain中(顺序可调换)
				for _, edge := range totalEqEdges {
					leftIdx := uint(nodeID2VisitID[edge.nodeIDs[0]])
					rightIdx := uint(nodeID2VisitID[edge.nodeIDs[1]])
					if ((sub & (1<<leftIdx)) > 0 && (remain & (1<<rightIdx)) > 0) || ((sub & (1<<rightIdx)) > 0 && (remain & (1<<leftIdx)) > 0) {
						usedEqEdges = append(usedEqEdges, edge)
					}
				}
				// 取出连接sub和remain的NonEqEdge: NonEqEdge的所有节点都出现在sub和remain中
				for _, edge := range subNonEqEdges {
					if edge.nodeIDMask & (sub | remain) != edge.nodeIDMask {
						continue
					}
					// Check whether this expression is only built from one side of the join.
					if sub & edge.nodeIDMask == edge.nodeIDMask || remain & edge.nodeIDMask == edge.nodeIDMask {
						continue
					}
					otherConds = append(otherConds, edge.expr)
				}
				// sub和remain间的EqEdges为空, 无法连接
				if len(usedEqEdges) == 0 {
					continue
				}
				join, err := s.newJoinWithEdge(bestPlan[sub].p, bestPlan[remain].p, usedEqEdges, otherConds)
				if err != nil {
					return nil, err
				}
				curCost := s.calcJoinCumCost(join, bestPlan[sub], bestPlan[remain])
				// 4.5.4 计算bestPlan[nodeBitmap]的最小值
				// 转移方程为 bestPlan[nodeBitmap] = min{join(bestPlan[sub], bestPlan[nodeBitmap^sub]), bestPlan[nodeBitmap]}, sub表示nodeBitmap的任意子集
				if bestPlan[nodeBitmap] == nil {
					bestPlan[nodeBitmap] = &jrNode{
						p:			join,
						cumCost: 	curCost,
					}
				} else if bestPlan[nodeBitmap].cumCost > curCost {
					bestPlan[nodeBitmap].p = join
					bestPlan[nodeBitmap].cumCost = curCost
				}
			}
		}
		// 4.5.5 连通子图的最优Join排序为bestPlan[1<<nodeCnt-1].p
		joins = append(joins, bestPlan[(1<<nodeCnt)-1].p)
	}
	// 5.最后为所有不包含等值条件的节点构建BushyJoinTree
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		remainedOtherConds = append(remainedOtherConds, edge.expr)
	}
	return s.makeBushyJoin(joins, remainedOtherConds), nil
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression) (LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds)
	_, err := join.recursiveDeriveStats()
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan, otherConds []expression.Expression) LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}
