/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"vitess.io/vitess/go/internal/global"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func gen4SelectStmtPlanner(
	query string,
	stmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {

	sel, isSel := stmt.(*sqlparser.Select)
	if isSel {
		// handle dual table for processing at vtgate.
		p, err := handleDualSelects(sel, vschema)
		if err != nil {
			return nil, err
		}
		if p != nil {
			used := "dual"
			keyspace, ksErr := vschema.DefaultKeyspace()
			if ksErr == nil {
				// we are just getting the ks to log the correct table use.
				// no need to fail this if we can't find the default keyspace
				used = keyspace.Name + ".dual"
			}
			return newPlanResult(p, used), nil
		}

		if sel.SQLCalcFoundRows && sel.Limit != nil {
			return gen4planSQLCalcFoundRows(vschema, sel, query, reservedVars)
		}
		// if there was no limit, we can safely ignore the SQLCalcFoundRows directive
		sel.SQLCalcFoundRows = false
	}

	plan, err := buildPlanForBypass(stmt, reservedVars, vschema)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func gen4planSQLCalcFoundRows(vschema plancontext.VSchema, sel *sqlparser.Select, query string, reservedVars *sqlparser.ReservedVars) (*planResult, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(sel, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	plan, tablesUsed, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, vschema, planSelectGen4)
	if err != nil {
		return nil, err
	}
	return newPlanResult(plan.Primitive(), tablesUsed...), nil
}

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, []string, error) {
	plan, _, tablesUsed, err := newBuildSelectPlan(sel, reservedVars, vschema, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	return nil, plan, tablesUsed, nil
}

func newBuildSelectPlan(
	selStmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (plan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	ksName := ""
	usingKs, err := GetUsingKs(selStmt, vschema)
	if err != nil {
		return nil, nil, nil, vterrors.VT09005()
	}
	ksName = usingKs.Name
	semTable, err = semantics.Analyze(selStmt, ksName, vschema)
	if err != nil {
		return nil, nil, nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	plan, tablesUsed, err = pushdownShortcut(ctx, selStmt, usingKs)
	if err != nil {
		return nil, nil, nil, err
	}
	plan, err = pushCommentDirectivesOnPlan(plan, selStmt)
	if err != nil {
		return nil, nil, nil, err
	}
	return plan, semTable, tablesUsed, err
}

// GetUsingKs returns the keyspace to use for the query.
func GetUsingKs(node sqlparser.SQLNode, vschema plancontext.VSchema) (*vindexes.Keyspace, error) {
	// If the query has a USING clause, it returns the keyspace specified in the clause.
	usingKs, err := vschema.DefaultKeyspace()
	if err == nil {
		return usingKs, nil
	}

	// If the query doesn't lookup any tables, it returns the default keyspace.
	allTables := sqlparser.GetAllTableNames(node)
	if len(allTables) == 1 && allTables[0].Name.String() == "dual" {
		return vschema.FindKeyspace(global.DefaultKeyspace)
	}

	// Otherwise, it returns the default keyspace, the default keyspace is the first keyspace in the query.
	firstKsName := ""
	for _, table := range allTables {
		if table.Qualifier.IsEmpty() {
			continue
		}
		firstKsName = table.Qualifier.String()
	}
	if firstKsName == "" {
		return nil, vterrors.VT09005()
	}
	usingKs, err = vschema.FindKeyspace(firstKsName)
	if err != nil {
		return nil, err
	}
	return usingKs, nil
}

func planLimit(limit *sqlparser.Limit, plan logicalPlan) (logicalPlan, error) {
	if limit == nil {
		return plan, nil
	}
	rb, ok := plan.(*routeGen4)
	if ok && rb.isSingleShard() {
		rb.SetLimit(limit)
		return plan, nil
	}

	lPlan, err := createLimit(plan, limit)
	if err != nil {
		return nil, err
	}

	// visit does not modify the plan.
	_, err = visit(lPlan, setUpperLimit)
	if err != nil {
		return nil, err
	}
	return lPlan, nil
}

func planHorizon(ctx *plancontext.PlanningContext, plan logicalPlan, in sqlparser.SelectStatement, truncateColumns bool) (logicalPlan, error) {
	switch node := in.(type) {
	case *sqlparser.Select:
		hp := horizonPlanning{
			sel: node,
		}

		replaceSubQuery(ctx, node)
		var err error
		plan, err = hp.planHorizon(ctx, plan, truncateColumns)
		if err != nil {
			return nil, err
		}
		plan, err = planLimit(node.Limit, plan)
		if err != nil {
			return nil, err
		}
	case *sqlparser.Union:
		var err error
		rb, isRoute := plan.(*routeGen4)
		if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
			return nil, ctx.SemTable.NotSingleRouteErr
		}
		if isRoute && rb.isSingleShard() {
			err = planSingleShardRoutePlan(node, rb)
		} else {
			plan, err = planOrderByOnUnion(ctx, plan, node)
		}
		if err != nil {
			return nil, err
		}

		plan, err = planLimit(node.Limit, plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil

}

func planOrderByOnUnion(ctx *plancontext.PlanningContext, plan logicalPlan, union *sqlparser.Union) (logicalPlan, error) {
	qp, err := operators.CreateQPFromUnion(union)
	if err != nil {
		return nil, err
	}
	hp := horizonPlanning{
		qp: qp,
	}
	if len(qp.OrderExprs) > 0 {
		plan, err = hp.planOrderBy(ctx, qp.OrderExprs, plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func pushCommentDirectivesOnPlan(plan logicalPlan, stmt sqlparser.Statement) (logicalPlan, error) {
	var directives *sqlparser.CommentDirectives
	cmt, ok := stmt.(sqlparser.Commented)
	if ok {
		directives = cmt.GetParsedComments().Directives()
		scatterAsWarns := directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings)
		timeout := queryTimeout(directives)

		if scatterAsWarns || timeout > 0 {
			_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
				switch plan := logicalPlan.(type) {
				case *routeGen4:
					plan.eroute.ScatterErrorsAsWarnings = scatterAsWarns
					plan.eroute.QueryTimeout = timeout
				}
				return true, logicalPlan, nil
			})
		}
	}

	return plan, nil
}
