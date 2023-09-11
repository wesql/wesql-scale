/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Error messages for CreateView queries
const (
	ViewDifferentKeyspace string = "Select query does not belong to the same keyspace as the view statement"
	ViewComplex           string = "Complex select queries are not supported in create or alter view statements"
	DifferentDestinations string = "Tables or Views specified in the query do not belong to the same destination"
)

type fkStrategy int

const (
	fkAllow fkStrategy = iota
	fkDisallow
)

var fkStrategyMap = map[string]fkStrategy{
	"allow":    fkAllow,
	"disallow": fkDisallow,
}

type fkContraint struct {
	found bool
}

func (fk *fkContraint) FkWalk(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable,
		*sqlparser.TableSpec, *sqlparser.AddConstraintDefinition, *sqlparser.ConstraintDefinition:
		return true, nil
	case *sqlparser.ForeignKeyDefinition:
		fk.found = true
	}
	return false, nil
}

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave completely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	normalDDLPlan, onlineDDLPlan, err := buildDDLPlans(sql, ddlStatement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}

	if ddlStatement.IsTemporary() {
		err := vschema.ErrorIfShardedF(normalDDLPlan.Keyspace, "temporary table", "Temporary table not supported in sharded database %s", normalDDLPlan.Keyspace.Name)
		if err != nil {
			return nil, err
		}
		onlineDDLPlan = nil // emptying this so it does not accidentally gets used somewhere
	}

	eddl := &engine.DDL{
		Keyspace:  normalDDLPlan.Keyspace,
		SQL:       normalDDLPlan.Query,
		DDL:       ddlStatement,
		NormalDDL: normalDDLPlan,
		OnlineDDL: onlineDDLPlan,

		DirectDDLEnabled: enableDirectDDL,
		OnlineDDLEnabled: enableOnlineDDL,

		CreateTempTable: ddlStatement.IsTemporary(),
	}
	tc := &tableCollector{}
	for _, tbl := range ddlStatement.AffectedTables() {
		tc.addASTTable(normalDDLPlan.Keyspace.Name, tbl)
	}

	return newPlanResult(eddl, tc.getTables()...), nil
}

func buildByPassDDLPlan(sql string, vschema plancontext.VSchema) (*planResult, error) {
	destination := vschema.Destination()
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil && err.Error() != vterrors.VT09005().Error() {
		return nil, err
	}
	send := buildNormalDDLPlan(keyspace, destination, sql)
	return newPlanResult(send), nil
}

func buildNormalDDLPlan(keyspace *vindexes.Keyspace, destination key.Destination, sql string) *engine.Send {
	send := &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql,
	}
	return send
}

func buildDDLPlans(sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*engine.Send, *engine.OnlineDDL, error) {
	destination := vschema.Destination()
	if destination == nil {
		destination = key.DestinationAllShards{}
	}
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil && err.Error() != vterrors.VT09005().Error() {
		return nil, nil, err
	}

	switch ddl := ddlStatement.(type) {
	case *sqlparser.AlterTable, *sqlparser.CreateTable, *sqlparser.TruncateTable:
		err = checkFKError(vschema, ddlStatement)
		if err != nil {
			return nil, nil, err
		}
		// For ALTER TABLE and TRUNCATE TABLE, the table must already exist
		//
		// For CREATE TABLE, the table may (in the case of --declarative)
		// already exist.
		//
		// We should find the target of the query from this tables location.
		_, _, err = findTableDestinationAndKeyspace(vschema, ddlStatement)
	case *sqlparser.CreateView:
		_, _, err = buildCreateView(vschema, ddl, reservedVars, enableOnlineDDL, enableDirectDDL)
	case *sqlparser.AlterView:
		_, _, err = buildAlterView(vschema, ddl, reservedVars, enableOnlineDDL, enableDirectDDL)
	case *sqlparser.DropView:
		_, _, err = buildDropView(vschema, ddlStatement)
	case *sqlparser.DropTable:
		_, _, err = buildDropTable(vschema, ddlStatement)
	case *sqlparser.RenameTable:
		_, _, err = buildRenameTable(vschema, ddl)
	default:
		return nil, nil, vterrors.VT13001(fmt.Sprintf("unexpected DDL statement type: %T", ddlStatement))
	}
	if err != nil {
		return nil, nil, err
	}

	query := sql
	// If the query is fully parsed, generate the query from the ast. Otherwise, use the original query
	if ddlStatement.IsFullyParsed() {
		query = sqlparser.String(ddlStatement)
	}

	normalDDL := buildNormalDDLPlan(keyspace, destination, sql)
	onlineDDL := &engine.OnlineDDL{
		Keyspace:          keyspace,
		TargetDestination: destination,
		DDL:               ddlStatement,
		SQL:               query,
	}
	return normalDDL, onlineDDL, nil
}

func checkFKError(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) error {
	if fkStrategyMap[vschema.ForeignKeyMode()] == fkDisallow {
		fk := &fkContraint{}
		_ = sqlparser.Walk(fk.FkWalk, ddlStatement)
		if fk.found {
			return vterrors.VT10001()
		}
	}
	return nil
}

func findTableDestinationAndKeyspace(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.Destination, *vindexes.Keyspace, error) {
	var table *vindexes.Table
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	var err error
	table, _, _, _, destination, err = vschema.FindTableOrVindex(ddlStatement.GetTable())
	if err != nil {
		_, isNotFound := err.(vindexes.NotFoundError)
		if !isNotFound {
			return nil, nil, err
		}
	}
	if table == nil {
		destination, keyspace, _, err = vschema.TargetDestination(ddlStatement.GetTable().Qualifier.String())
		if err != nil {
			return nil, nil, err
		}
		ddlStatement.SetTable("", ddlStatement.GetTable().Name.String())
	} else {
		keyspace = table.Keyspace
		ddlStatement.SetTable("", table.Name.String())
	}
	return destination, keyspace, nil
}

func buildAlterView(vschema plancontext.VSchema, ddl *sqlparser.AlterView, reservedVars *sqlparser.ReservedVars, enableOnlineDDL, enableDirectDDL bool) (key.Destination, *vindexes.Keyspace, error) {
	// For Alter View, we require that the view exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, err := findTableDestinationAndKeyspace(vschema, ddl)
	if err != nil {
		return nil, nil, err
	}

	selectPlan, err := createInstructionFor(sqlparser.String(ddl.Select), ddl.Select, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, nil, err
	}
	selPlanKs := selectPlan.primitive.GetKeyspaceName()
	if keyspace.Name != selPlanKs {
		return nil, nil, vterrors.VT12001(ViewDifferentKeyspace)
	}
	if vschema.IsViewsEnabled() {
		if keyspace == nil {
			return nil, nil, vterrors.VT09005()
		}
		return destination, keyspace, nil
	}
	isRoutePlan, opCode := tryToGetRoutePlan(selectPlan.primitive)
	if !isRoutePlan {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	if opCode != engine.Unsharded && opCode != engine.EqualUnique && opCode != engine.Scatter {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	_ = sqlparser.SafeRewrite(ddl.Select, nil, func(cursor *sqlparser.Cursor) bool {
		switch tableName := cursor.Node().(type) {
		case sqlparser.TableName:
			cursor.Replace(sqlparser.TableName{
				Name: tableName.Name,
			})
		}
		return true
	})
	return destination, keyspace, nil
}

func buildCreateView(vschema plancontext.VSchema, ddl *sqlparser.CreateView, reservedVars *sqlparser.ReservedVars, enableOnlineDDL, enableDirectDDL bool) (key.Destination, *vindexes.Keyspace, error) {
	// For Create View, we require that the keyspace exist and the select query can be satisfied within the keyspace itself
	// We should remove the keyspace name from the table name, as the database name in MySQL might be different than the keyspace name
	destination, keyspace, _, err := vschema.TargetDestination(ddl.ViewName.Qualifier.String())
	if err != nil {
		return nil, nil, err
	}
	ddl.ViewName.Qualifier = sqlparser.NewIdentifierCS("")

	selectPlan, err := createInstructionFor(sqlparser.String(ddl.Select), ddl.Select, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, nil, err
	}
	selPlanKs := selectPlan.primitive.GetKeyspaceName()
	if keyspace.Name != selPlanKs {
		return nil, nil, vterrors.VT12001(ViewDifferentKeyspace)
	}
	if vschema.IsViewsEnabled() {
		if keyspace == nil {
			return nil, nil, vterrors.VT09005()
		}
		return destination, keyspace, nil
	}
	isRoutePlan, opCode := tryToGetRoutePlan(selectPlan.primitive)
	if !isRoutePlan {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	if opCode != engine.Unsharded && opCode != engine.EqualUnique && opCode != engine.Scatter {
		return nil, nil, vterrors.VT12001(ViewComplex)
	}
	_ = sqlparser.SafeRewrite(ddl.Select, nil, func(cursor *sqlparser.Cursor) bool {
		switch tableName := cursor.Node().(type) {
		case sqlparser.TableName:
			cursor.Replace(sqlparser.TableName{
				Name: tableName.Name,
			})
		}
		return true
	})
	return destination, keyspace, nil
}

func buildDropView(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.Destination, *vindexes.Keyspace, error) {
	if !vschema.IsViewsEnabled() {
		return buildDropTable(vschema, ddlStatement)
	}
	var ks *vindexes.Keyspace
	viewMap := make(map[string]any)
	for _, tbl := range ddlStatement.GetFromTables() {
		_, ksForView, _, err := vschema.TargetDestination(tbl.Qualifier.String())
		if err != nil {
			return nil, nil, err
		}
		if ksForView == nil {
			return nil, nil, vterrors.VT09005()
		}
		if ks == nil {
			ks = ksForView
		} else if ks.Name != ksForView.Name {
			return nil, nil, vterrors.VT12001("cannot drop views from multiple keyspace in a single statement")
		}
		if _, exists := viewMap[tbl.Name.String()]; exists {
			return nil, nil, vterrors.VT03013(tbl.Name.String())
		}
		viewMap[tbl.Name.String()] = nil
		tbl.Qualifier = sqlparser.NewIdentifierCS("")
	}
	return key.DestinationAllShards{}, ks, nil
}

func buildDropTable(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) (key.Destination, *vindexes.Keyspace, error) {
	var destination key.Destination
	var keyspace *vindexes.Keyspace
	for i, tab := range ddlStatement.GetFromTables() {
		var destinationTab key.Destination
		var keyspaceTab *vindexes.Keyspace
		var table *vindexes.Table
		var err error
		table, _, _, _, destinationTab, err = vschema.FindTableOrVindex(tab)

		if err != nil {
			_, isNotFound := err.(vindexes.NotFoundError)
			if !isNotFound {
				return nil, nil, err
			}
		}
		if table == nil {
			destinationTab, keyspaceTab, _, err = vschema.TargetDestination(tab.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			ddlStatement.GetFromTables()[i] = sqlparser.TableName{
				Name: tab.Name,
			}
		} else {
			keyspaceTab = table.Keyspace
			ddlStatement.GetFromTables()[i] = sqlparser.TableName{
				Name: table.Name,
			}
		}

		if destination == nil && keyspace == nil {
			destination = destinationTab
			keyspace = keyspaceTab
		}
		if destination != destinationTab || keyspace != keyspaceTab {
			return nil, nil, vterrors.VT12001(DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}

func buildRenameTable(vschema plancontext.VSchema, renameTable *sqlparser.RenameTable) (key.Destination, *vindexes.Keyspace, error) {
	var destination key.Destination
	var keyspace *vindexes.Keyspace

	for _, tabPair := range renameTable.TablePairs {
		var destinationFrom key.Destination
		var keyspaceFrom *vindexes.Keyspace
		var table *vindexes.Table
		var err error
		table, _, _, _, destinationFrom, err = vschema.FindTableOrVindex(tabPair.FromTable)

		if err != nil {
			_, isNotFound := err.(vindexes.NotFoundError)
			if !isNotFound {
				return nil, nil, err
			}
		}
		if table == nil {
			destinationFrom, keyspaceFrom, _, err = vschema.TargetDestination(tabPair.FromTable.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			tabPair.FromTable = sqlparser.TableName{
				Name: tabPair.FromTable.Name,
			}
		} else {
			keyspaceFrom = table.Keyspace
			tabPair.FromTable = sqlparser.TableName{
				Name: table.Name,
			}
		}

		if tabPair.ToTable.Qualifier.String() != "" {
			_, keyspaceTo, _, err := vschema.TargetDestination(tabPair.ToTable.Qualifier.String())
			if err != nil {
				return nil, nil, err
			}
			if keyspaceTo.Name != keyspaceFrom.Name {
				return nil, nil, vterrors.VT03002(keyspaceFrom.Name, keyspaceTo.Name)
			}
			tabPair.ToTable = sqlparser.TableName{
				Name: tabPair.ToTable.Name,
			}
		}

		if destination == nil && keyspace == nil {
			destination = destinationFrom
			keyspace = keyspaceFrom
		}
		if destination != destinationFrom || keyspace != keyspaceFrom {
			return nil, nil, vterrors.VT12001(DifferentDestinations)
		}
	}
	return destination, keyspace, nil
}

func tryToGetRoutePlan(selectPlan engine.Primitive) (valid bool, opCode engine.Opcode) {
	switch plan := selectPlan.(type) {
	case *engine.Route:
		return true, plan.Opcode
	case engine.Gen4Comparer:
		return tryToGetRoutePlan(plan.GetGen4Primitive())
	default:
		return false, engine.Opcode(0)
	}
}
