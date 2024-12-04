package engine

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/branch"
)

var _ Primitive = (*Branch)(nil)

type BranchCommandType string

const (
	Create           BranchCommandType = "create"
	Diff             BranchCommandType = "diff"
	MergeBack        BranchCommandType = "mergeBack"
	PrepareMergeBack BranchCommandType = "prepareMergeBack"
	Cleanup          BranchCommandType = "cleanUp"
	Show             BranchCommandType = "show"
)

const (
	DefaultBranchName = "my_branch"

	DefaultBranchTargetHost     = "127.0.0.1"
	DefaultBranchTargetPort     = 15306
	DefaultBranchTargetUser     = "root"
	DefaultBranchTargetPassword = ""
)

// Branch is an operator to deal with branch commands
type Branch struct {
	// set when plan building
	name        string
	commandType BranchCommandType
	params      branchParams

	noInputs
}

type branchParams interface {
	validate() error
	setValues(params map[string]string) error
}

const (
	BranchParamsName = "name"
)

const (
	BranchCreateParamsSourceHost      = "source_host"
	BranchCreateParamsSourcePort      = "source_port"
	BranchCreateParamsSourceUser      = "source_user"
	BranchCreateParamsSourcePassword  = "source_password"
	BranchCreateParamsInclude         = "include_databases"
	BranchCreateParamsExclude         = "exclude_databases"
	BranchCreateParamsTargetDBPattern = "target_db_pattern"
)

type BranchCreateParams struct {
	SourceHost      string
	SourcePort      string
	SourceUser      string
	SourcePassword  string
	Include         string
	Exclude         string
	TargetDBPattern string
}

const (
	BranchDiffParamsCompareObjects = "compare_objects"
)

type BranchDiffParams struct {
	CompareObjects string
}

const (
	BranchPrepareMergeBackParamsMergeOption = "merge_option"
)

type BranchPrepareMergeBackParams struct {
	MergeOption string
}

const (
	BranchShowParamsShowOption = "show_option"

	ShowStatus       = "status"
	ShowSnapshot     = "snapshot"
	ShowMergeBackDDL = "merge_back_ddl"
)

type BranchShowParams struct {
	ShowOption string
}

// ***************************************************************************************************************************************************

func BuildBranchPlan(branchCmd *sqlparser.BranchCommand) (*Branch, error) {
	// plan will be cached and index by sql template, so here we just build some static info related the sql.
	cmdType, err := parseBranchCommandType(branchCmd.Type)
	if err != nil {
		return nil, err
	}
	params := make(map[string]string)
	for i, _ := range branchCmd.Params.Keys {
		params[branchCmd.Params.Keys[i]] = branchCmd.Params.Values[i]
	}
	b := &Branch{commandType: cmdType}
	err = b.setAndValidateParams(params)
	if err != nil {
		return nil, fmt.Errorf("invalid branch command params: %w", err)
	}
	return b, nil
}

// TryExecute implements Primitive interface
func (b *Branch) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch b.commandType {
	case Create:
		return b.branchCreate()
	case Diff:
		return b.branchDiff()
	case PrepareMergeBack:
		return b.branchPrepareMergeBack()
	case MergeBack:
		return b.branchMergeBack()
	case Cleanup:
		return b.branchCleanUp()
	case Show:
		return b.branchShow()
	default:
		return nil, fmt.Errorf("unsupported branch command type: %s", b.commandType)
	}
}

// todo complete me
// TryStreamExecute implements Primitive interface
func (b *Branch) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result := &sqltypes.Result{}
	return callback(result)
}

// GetFields implements Primitive interface
func (b *Branch) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (b *Branch) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "Branch"
	return rst
}

// NeedsTransaction implements the Primitive interface
func (b *Branch) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (b *Branch) RouteType() string {
	return "Branch"
}

// GetKeyspaceName implements Primitive interface
func (b *Branch) GetKeyspaceName() string {
	return ""
}

// GetTableName implements Primitive interface
func (b *Branch) GetTableName() string {
	return ""
}

// ***************************************************************************************************************************************************

func parseBranchCommandType(s string) (BranchCommandType, error) {
	switch s {
	case string(Create):
		return Create, nil
	case string(Diff):
		return Diff, nil
	case string(MergeBack):
		return MergeBack, nil
	case string(PrepareMergeBack):
		return PrepareMergeBack, nil
	case string(Cleanup):
		return Cleanup, nil
	case string(Show):
		return Show, nil
	default:
		return "", fmt.Errorf("invalid branch command type: %s", s)
	}
}

func (b *Branch) setAndValidateParams(paramsMap map[string]string) error {
	if name, exists := paramsMap[BranchParamsName]; exists {
		b.name = name
		delete(paramsMap, BranchParamsName)
	} else {
		b.name = DefaultBranchName
	}

	var params branchParams
	switch b.commandType {
	case Create:
		params = &BranchCreateParams{}
	case Diff:
		params = &BranchDiffParams{}
	case PrepareMergeBack:
		params = &BranchPrepareMergeBackParams{}
	case Show:
		params = &BranchShowParams{}
	case MergeBack, Cleanup:
		return nil
	default:
		return fmt.Errorf("invalid branch command type: %s", b.commandType)
	}
	err := params.setValues(paramsMap)
	if err != nil {
		return err
	}
	err = params.validate()
	if err != nil {
		return err
	}
	b.params = params
	return nil
}

func checkRedundantParams(params map[string]string) error {
	if len(params) > 0 {
		invalidParams := make([]string, 0)
		for k := range params {
			invalidParams = append(invalidParams, k)
		}
		return fmt.Errorf("invalid params: %v", invalidParams)
	}
	return nil
}

func (bcp *BranchCreateParams) setValues(params map[string]string) error {
	if v, ok := params[BranchCreateParamsSourcePort]; ok {
		bcp.SourcePort = v
	} else {
		bcp.SourcePort = "3306"
		delete(params, BranchCreateParamsSourcePort)
	}

	if v, ok := params[BranchCreateParamsInclude]; ok {
		bcp.Include = v
		delete(params, BranchCreateParamsInclude)
	} else {
		bcp.Include = "*"
	}

	if v, ok := params[BranchCreateParamsExclude]; ok {
		bcp.Exclude = v
		delete(params, BranchCreateParamsExclude)
	} else {
		bcp.Exclude = "information_schema,mysql.performance_schema,sys"
	}

	if v, ok := params[BranchCreateParamsSourceHost]; ok {
		bcp.SourceHost = v
		delete(params, BranchCreateParamsSourceHost)
	}

	if v, ok := params[BranchCreateParamsSourceUser]; ok {
		bcp.SourceUser = v
		delete(params, BranchCreateParamsSourceUser)
	}

	if v, ok := params[BranchCreateParamsSourcePassword]; ok {
		bcp.SourcePassword = v
		delete(params, BranchCreateParamsSourcePassword)
	}

	if v, ok := params[BranchCreateParamsTargetDBPattern]; ok {
		bcp.TargetDBPattern = v
		delete(params, BranchCreateParamsTargetDBPattern)
	}

	return checkRedundantParams(params)
}

func (bcp *BranchCreateParams) validate() error {
	if bcp.SourceHost == "" {
		return fmt.Errorf("branch create: source host is required")
	}

	if bcp.SourcePort == "" {
		return fmt.Errorf("branch create: source port is required")
	} else {
		port, err := strconv.Atoi(bcp.SourcePort)
		if err != nil {
			return fmt.Errorf("branch create: source port is not a number")
		}
		if port <= 0 || port > 65535 {
			return fmt.Errorf("branch create: source port %v is not a valid port", bcp.SourcePort)
		}
	}

	if bcp.Include == "" {
		return fmt.Errorf("branch create: include databases is required")
	}

	return nil
}

// todo complete me
// todo output type
func (bdp *BranchDiffParams) setValues(params map[string]string) error {
	if v, ok := params[BranchDiffParamsCompareObjects]; ok {
		bdp.CompareObjects = v
		delete(params, BranchDiffParamsCompareObjects)
	} else {
		bdp.CompareObjects = string(branch.FromSourceToTarget)
	}

	return checkRedundantParams(params)
}

// todo complete me
// todo output type
func (bdp *BranchDiffParams) validate() error {
	switch branch.BranchDiffObjectsFlag(bdp.CompareObjects) {
	case branch.FromSourceToTarget, branch.FromTargetToSource,
		branch.FromSnapshotToSource, branch.FromSnapshotToTarget,
		branch.FromTargetToSnapshot, branch.FromSourceToSnapshot:
	default:
		return fmt.Errorf("invalid compare objects: %s", bdp.CompareObjects)
	}

	return nil
}

func (bpp *BranchPrepareMergeBackParams) setValues(params map[string]string) error {
	if v, ok := params[BranchPrepareMergeBackParamsMergeOption]; ok {
		bpp.MergeOption = v
		delete(params, BranchPrepareMergeBackParamsMergeOption)
	} else {
		bpp.MergeOption = string(branch.MergeOverride)
	}

	return checkRedundantParams(params)
}

func (bpp *BranchPrepareMergeBackParams) validate() error {
	// todo enhancement: support diff objects
	switch branch.MergeBackOption(bpp.MergeOption) {
	case branch.MergeOverride:
	default:
		return fmt.Errorf("invalid merge option: %s", bpp.MergeOption)
	}
	return nil
}

func (bsp *BranchShowParams) setValues(params map[string]string) error {
	if v, ok := params[BranchShowParamsShowOption]; ok {
		bsp.ShowOption = v
		delete(params, BranchShowParamsShowOption)
	} else {
		bsp.ShowOption = ShowStatus
	}

	return checkRedundantParams(params)
}

func (bsp *BranchShowParams) validate() error {
	switch bsp.ShowOption {
	case ShowSnapshot, ShowStatus, ShowMergeBackDDL:
	default:
		return fmt.Errorf("invalid merge option: %s", bsp.ShowOption)
	}
	return nil
}

func createBranchSourceHandler(sourceUser, sourcePassword, sourceHost string, sourcePort int) (*branch.SourceMySQLService, error) {
	sourceMysqlConfig := &mysql.Config{
		User:   sourceUser,
		Passwd: sourcePassword,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", sourceHost, sourcePort),
	}
	sourceMysqlService, err := branch.NewMysqlServiceWithConfig(sourceMysqlConfig)
	if err != nil {
		return nil, err
	}
	sourceMysqlHandler := branch.NewSourceMySQLService(sourceMysqlService)
	return sourceMysqlHandler, nil
}

func createBranchTargetHandler(targetUser, targetPassword, targetHost string, targetPort int) (*branch.TargetMySQLService, error) {
	targetMysqlConfig := &mysql.Config{
		User:   targetUser,
		Passwd: targetPassword,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", targetHost, targetPort),
	}
	targetMysqlService, err := branch.NewMysqlServiceWithConfig(targetMysqlConfig)
	if err != nil {
		return nil, err
	}
	targetMysqlHandler := branch.NewTargetMySQLService(targetMysqlService)
	return targetMysqlHandler, nil
}

func (b *Branch) branchCreate() (*sqltypes.Result, error) {
	// create branch meta
	createParams, ok := b.params.(*BranchCreateParams)
	if !ok {
		return nil, fmt.Errorf("branch create: invalid branch command params")
	}
	port, err := strconv.Atoi(createParams.SourcePort)
	if err != nil {
		return nil, err
	}
	branchMeta, err := branch.NewBranchMeta(b.name, createParams.SourceHost, port, createParams.SourceUser, createParams.SourcePassword, createParams.Include, createParams.Exclude, createParams.TargetDBPattern)
	if err != nil {
		return nil, err
	}

	// create branch service
	sourceHandler, err := createBranchSourceHandler(createParams.SourceUser, createParams.SourcePassword, createParams.SourceHost, port)
	if err != nil {
		return nil, err
	}
	targetHandler, err := createBranchTargetHandler(DefaultBranchTargetUser, DefaultBranchTargetPassword, DefaultBranchTargetHost, DefaultBranchTargetPort)
	if err != nil {
		return nil, err
	}
	bs := branch.NewBranchService(sourceHandler, targetHandler)

	// create branch
	return &sqltypes.Result{}, bs.BranchCreate(branchMeta)
}

func (b *Branch) branchDiff() (*sqltypes.Result, error) {
	diffParams, ok := b.params.(*BranchDiffParams)
	if !ok {
		return nil, fmt.Errorf("branch diff: invalid branch command params")
	}
	meta, bs, _, _, err := getBranchDataStruct(b.name)
	if err != nil {
		return nil, err
	}

	// todo enhancement: support diff hints?
	diff, err := bs.BranchDiff(meta.Name, meta.IncludeDatabases, meta.ExcludeDatabases, branch.BranchDiffObjectsFlag(diffParams.CompareObjects), &schemadiff.DiffHints{})
	if err != nil {
		return nil, err
	}

	return buildBranchDiffResult(diff), nil
}

func (b *Branch) branchPrepareMergeBack() (*sqltypes.Result, error) {
	prepareMergeBackParams, ok := b.params.(*BranchPrepareMergeBackParams)
	if !ok {
		return nil, fmt.Errorf("branch prepare merge back: invalid branch command params")
	}

	meta, bs, _, _, err := getBranchDataStruct(b.name)
	if err != nil {
		return nil, err
	}

	// todo enhancement: support diff hints?
	diff, err := bs.BranchPrepareMergeBack(meta.Name, meta.Status, meta.IncludeDatabases, meta.ExcludeDatabases, branch.MergeBackOption(prepareMergeBackParams.MergeOption), &schemadiff.DiffHints{})
	if err != nil {
		return nil, err
	}

	return buildBranchDiffResult(diff), nil
}

func (b *Branch) branchMergeBack() (*sqltypes.Result, error) {
	meta, bs, _, _, err := getBranchDataStruct(b.name)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, bs.BranchMergeBack(meta.Name, meta.Status)
}

func (b *Branch) branchCleanUp() (*sqltypes.Result, error) {
	meta, bs, _, _, err := getBranchDataStruct(b.name)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, bs.BranchCleanUp(meta.Name)
}

func (b *Branch) branchShow() (*sqltypes.Result, error) {
	showParams, ok := b.params.(*BranchShowParams)
	if !ok {
		return nil, fmt.Errorf("branch show: invalid branch command params")
	}

	meta, _, _, targetHandler, err := getBranchDataStruct(b.name)
	if err != nil {
		return nil, err
	}

	switch showParams.ShowOption {
	case ShowStatus:
		return buildMetaResult(meta)
	case ShowSnapshot:
		return buildSnapshotResult(meta.Name, targetHandler)
	case ShowMergeBackDDL:
		return buildMergeBackDDLResult(meta.Name, targetHandler)
	default:
		return nil, fmt.Errorf("branch show: invalid branch command params")
	}
}

func getBranchDataStruct(name string) (*branch.BranchMeta, *branch.BranchService, *branch.SourceMySQLService, *branch.TargetMySQLService, error) {
	// get target handler
	targetHandler, err := createBranchTargetHandler(DefaultBranchTargetUser, DefaultBranchTargetPassword, DefaultBranchTargetHost, DefaultBranchTargetPort)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// get branch meta
	meta, err := targetHandler.SelectAndValidateBranchMeta(name)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// get source handler
	sourceHandler, err := createBranchSourceHandler(meta.SourceUser, meta.SourcePassword, meta.SourceHost, meta.SourcePort)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// get branch service
	bs := branch.NewBranchService(sourceHandler, targetHandler)
	return meta, bs, sourceHandler, targetHandler, nil
}

func buildBranchDiffResult(diff *branch.BranchDiff) *sqltypes.Result {
	fields := sqltypes.BuildVarCharFields("database", "table", "ddl")
	rows := make([][]sqltypes.Value, 0)
	for db, dbDiff := range diff.Diffs {
		if dbDiff.NeedDropDatabase {
			rows = append(rows, sqltypes.BuildVarCharRow(db, "", fmt.Sprintf("drop database `%s`", db)))
			continue
		}
		if dbDiff.NeedCreateDatabase {
			rows = append(rows, sqltypes.BuildVarCharRow(db, "", fmt.Sprintf("create database `%s`", db)))
		}
		for table, tableDiffs := range dbDiff.TableDDLs {
			for _, tableDiff := range tableDiffs {
				rows = append(rows, sqltypes.BuildVarCharRow(db, table, tableDiff))
			}
		}
	}
	return &sqltypes.Result{Fields: fields, Rows: rows}
}

func buildMetaResult(meta *branch.BranchMeta) (*sqltypes.Result, error) {
	fields := sqltypes.BuildVarCharFields("name", "status", "source host", "source port", "source user", "include", "exclude", "target db pattern")
	rows := make([][]sqltypes.Value, 0)
	include := strings.Join(meta.IncludeDatabases, ",")
	exclude := strings.Join(meta.ExcludeDatabases, ",")
	rows = append(rows, sqltypes.BuildVarCharRow(meta.Name, string(meta.Status), meta.SourceHost, string(rune(meta.SourcePort)), meta.SourceUser, include, exclude, meta.TargetDBPattern))

	return &sqltypes.Result{Fields: fields, Rows: rows}, nil
}

func buildMergeBackDDLResult(branchName string, targetHandler *branch.TargetMySQLService) (*sqltypes.Result, error) {
	sql := branch.GetSelectMergeBackDDLSQL(branchName)
	service := targetHandler.GetMysqlService()
	rows, err := service.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := sqltypes.BuildVarCharFields("id", "name", "database", "table", "ddl", "merged")
	resultRows := make([][]sqltypes.Value, 0)
	for rows.Next() {
		var (
			id       int
			name     string
			database string
			table    string
			ddl      string
			merged   bool
		)

		if err := rows.Scan(&id, &name, &database, &table, &ddl, &merged); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		mergedStr := "false"
		if merged {
			mergedStr = "true"
		}
		resultRows = append(resultRows, sqltypes.BuildVarCharRow(string(rune(id)), name, database, table, ddl, mergedStr))
	}

	return &sqltypes.Result{Fields: fields, Rows: resultRows}, nil
}

func buildSnapshotResult(branchName string, targetHandler *branch.TargetMySQLService) (*sqltypes.Result, error) {
	selectSnapshotSQL := branch.GetSelectSnapshotSQL(branchName)
	mysqlService := targetHandler.GetMysqlService()
	rows, err := mysqlService.Query(selectSnapshotSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %v: %v", selectSnapshotSQL, err)
	}
	defer rows.Close()

	fields := sqltypes.BuildVarCharFields("id", "name", "database", "table", "create table")
	resultRows := make([][]sqltypes.Value, 0)

	for rows.Next() {
		var (
			id             int
			name           string
			database       string
			table          string
			createTableSQL string
		)

		if err := rows.Scan(&id, &name, &database, &table, &createTableSQL); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		resultRows = append(resultRows, sqltypes.BuildVarCharRow(string(rune(id)), name, database, table, createTableSQL))
	}

	return &sqltypes.Result{Fields: fields, Rows: resultRows}, nil
}