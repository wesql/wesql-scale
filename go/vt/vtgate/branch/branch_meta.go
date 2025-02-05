package branch

import "vitess.io/vitess/go/vt/schemadiff"

type BranchMeta struct {
	Name string
	// source info
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	// filter rules
	IncludeDatabases []string
	ExcludeDatabases []string
	// others
	Status BranchStatus
}

type BranchStatus string

const (
	StatusUnknown BranchStatus = "unknown"
	StatusInit    BranchStatus = "init"
	StatusFetched BranchStatus = "fetched"
	StatusCreated BranchStatus = "created"

	StatusPreparing BranchStatus = "preparing"
	StatusPrepared  BranchStatus = "prepared"
	StatusMerging   BranchStatus = "merging"
	StatusMerged    BranchStatus = "merged"
)

func StringToBranchStatus(s string) BranchStatus {
	switch s {
	case "init":
		return StatusInit
	case "fetched":
		return StatusFetched
	case "created":
		return StatusCreated
	case "preparing":
		return StatusPreparing
	case "prepared":
		return StatusPrepared
	case "merging":
		return StatusMerging
	case "merged":
		return StatusMerged
	default:
		return StatusUnknown
	}
}

type MergeBackOption string

const (
	MergeOverride MergeBackOption = "override"
	MergeDiff     MergeBackOption = "diff"
)

type BranchDiffObjectsFlag string

const (
	FromSourceToTarget   BranchDiffObjectsFlag = "source_target" // diff from source schema to target schema
	FromTargetToSource   BranchDiffObjectsFlag = "target_source"
	FromSourceToSnapshot BranchDiffObjectsFlag = "source_snapshot"
	FromSnapshotToSource BranchDiffObjectsFlag = "snapshot_source"
	FromTargetToSnapshot BranchDiffObjectsFlag = "target_snapshot"
	FromSnapshotToTarget BranchDiffObjectsFlag = "snapshot_target"
)

type BranchSchema struct {
	// databases -> tables -> create table statement or DDLs
	branchSchema map[string]map[string]string
}

type DatabaseDiff struct {
	NeedCreateDatabase bool
	NeedDropDatabase   bool
	// table Name -> ddls to create, drop or alter this table from origin to expected
	TableDDLs map[string][]string

	// table Name -> EntityDiffs, used in schema merge back conflict check
	tableEntityDiffs map[string]schemadiff.EntityDiff
}

type BranchDiff struct {
	// database Name -> DatabaseDiff
	Diffs map[string]*DatabaseDiff
}

const (
	SelectBatchSize = 5000

	// branch meta related

	UpsertBranchMetaSQL = `
    INSERT INTO mysql.branch 
        (Name, source_host, source_port, source_user, source_password, 
        include_databases, exclude_databases, Status) 
    VALUES 
        (%a, %a, %a, %a, %a, %a, %a, %a)
    ON DUPLICATE KEY UPDATE 
        source_host = VALUES(source_host),
        source_port = VALUES(source_port),
        source_user = VALUES(source_user),
        source_password = VALUES(source_password),
        include_databases = VALUES(include_databases),
        exclude_databases = VALUES(exclude_databases),
        Status = VALUES(Status)`

	SelectBranchMetaSQL = "select * from mysql.branch where Name=%a"

	InsertBranchMetaSQL = `INSERT INTO mysql.branch 
        (Name, source_host, source_port, source_user, source_password, 
        include_databases, exclude_databases, Status) 
    VALUES 
        (%a, %a, %a, %a, %a, %a, %a, %a)`

	UpdateBranchStatusSQL = "update mysql.branch set Status=%a where Name=%a"

	DeleteBranchMetaSQL = "delete from mysql.branch where Name=%a"

	// snapshot related

	SelectBranchSnapshotInBatchSQL = "select * from mysql.branch_snapshot where Name=%a and id > %a order by id asc limit %a"

	DeleteBranchSnapshotSQL = "delete from mysql.branch_snapshot where Name=%a"

	InsertBranchSnapshotSQL = "insert into mysql.branch_snapshot (`Name`, `database`, `table`, `create_table_sql`) values (%a, %a, %a, %a)"

	// merge back ddl related

	DeleteBranchMergeBackDDLSQL = "delete from mysql.branch_patch where Name=%a"

	SelectBranchUnmergedDDLInBatchSQL = "select * from mysql.branch_patch where Name=%a and merged = false and id > %a order by id asc limit %a"

	SelectBranchUnmergedDBDDLInBatchSQL = "select * from mysql.branch_patch where Name=%a and merged = false and `table` = '' and id > %a order by id asc limit %a"

	SelectBranchMergeBackDDLInBatchSQL = "select * from mysql.branch_patch where Name=%a and id > %a order by id asc limit %a"

	InsertBranchMergeBackDDLSQL = "insert into mysql.branch_patch (`Name`, `database`, `table`, `ddl`, `merged`) values (%a, %a, %a, %a, false)"

	UpdateBranchMergeBackDDLMergedSQL = "update mysql.branch_patch set merged = true where id = %a"
)
