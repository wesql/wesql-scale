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

package scheduler

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	anyErrorIndicator = "<any-error-of-any-kind>"
)

type testOnlineDDLStatementParams struct {
	ddlStatement     string
	ddlStrategy      string
	expectHint       string
	expectError      string
	skipWait         bool
	migrationContext string
}

type testRevertMigrationParams struct {
	revertUUID       string
	executeStrategy  string
	ddlStrategy      string
	migrationContext string
	expectError      string
	skipWait         bool
}

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	normalWaitTime            = 20 * time.Second
	extendedWaitTime          = 60 * time.Second
	ensureStateNotChangedTime = 5 * time.Second

	hostname              = "localhost"
	keyspaceName          = "mysql"
	DBName                = "test"
	cell                  = "zone1"
	schemaChangeDirectory = ""
)

func parseTableName(t *testing.T, sql string) (tableName string) {
	// ddlStatement could possibly be composed of multiple DDL statements
	tokenizer := sqlparser.NewStringTokenizer(sql)
	for {
		stmt, err := sqlparser.ParseNextStrictDDL(tokenizer)
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		require.NoErrorf(t, err, "parsing sql: [%v]", sql)
		ddlStmt, ok := stmt.(sqlparser.DDLStatement)
		require.True(t, ok)
		tableName = ddlStmt.GetTable().Name.String()
		if tableName == "" {
			tbls := ddlStmt.AffectedTables()
			require.NotEmpty(t, tbls)
			tableName = tbls[0].Name.String()
		}
		require.NotEmptyf(t, tableName, "could not parse table name from SQL: %s", sqlparser.String(ddlStmt))
	}
	require.NotEmptyf(t, tableName, "could not parse table name from SQL: %s", sql)
	return tableName
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func TestParseTableName(t *testing.T) {
	sqls := []string{
		`ALTER TABLE t1_test ENGINE=InnoDB`,
		`ALTER TABLE t1_test ENGINE=InnoDB;`,
		`DROP TABLE IF EXISTS t1_test`,
		`
			ALTER TABLE stress_test ENGINE=InnoDB;
			ALTER TABLE stress_test ENGINE=InnoDB;
			ALTER TABLE stress_test ENGINE=InnoDB;
		`,
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			parseTableName(t, sql)
		})
	}
}

func initSchema(t *testing.T) {
	createSchemaSQL := "create database %s"
	createVtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	query := onlineddl.VtgateExecQuery(t, &createVtParams, fmt.Sprintf(createSchemaSQL, DBName), "")
	require.Equal(t, 1, int(query.RowsAffected))
}

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema_change_dir", schemaChangeDirectory,
			"--schema_change_controller", "local",
			"--schema_change_check_interval", "1"}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--watch_replication_stream",
		}
		clusterInstance.VtGateExtraArgs = []string{}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		// No need for replicas in this stress test
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"0"}, 0, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: DBName,
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestSchemaChange(t *testing.T) {
	t.Run("initSchema", initSchema)
	t.Run("scheduler", testScheduler)
	t.Run("pause", testPause)
}

func testRows(t *testing.T, tableName string, expectedRows int64) {
	sqlQuery := fmt.Sprintf("SELECT COUNT(*) AS c FROM %s ;", tableName)
	r := onlineddl.VtgateExecQuery(t, &vtParams, sqlQuery, "")
	require.NotNil(t, r)
	row := r.Named().Row()
	require.NotNil(t, row)
	require.Equal(t, expectedRows, row.AsInt64("c", 0))
}

func checkTableColExist(t *testing.T, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		fmt.Printf("table create statement is %s\n", createStatement)
		assert.Contains(t, createStatement, expectColumn)
	}
}

func checkTableColNotExist(t *testing.T, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		fmt.Printf("table create statement is %s\n", createStatement)
		assert.NotContains(t, createStatement, expectColumn)
	}
}

// check the state before pause value of a given migration
func checkStateBeforePauseOfMigration(t *testing.T, uuid, expectState string) {
	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	assert.Equal(t, 1, len(rs.Named().Rows))
	assert.Equal(t, expectState, rs.Named().Rows[0].AsString("status_before_paused", ""))
}

func getArtifactsOfMigration(t *testing.T, uuid string) string {
	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	assert.Equal(t, 1, len(rs.Named().Rows))
	return rs.Named().Rows[0].AsString("artifacts", "")
}

func checkArtifactsOfMigration(t *testing.T, uuid, expectArtifacts string) {
	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	assert.Equal(t, 1, len(rs.Named().Rows))
	actualArtifacts := rs.Named().Rows[0].AsString("artifacts", "")
	fmt.Printf("expectArtifacts is %s, actualArtifacts is %s", expectArtifacts, actualArtifacts)
	assert.Equal(t, expectArtifacts, actualArtifacts)
}

func WaitForVreplicationState(t *testing.T, vtParams *mysql.ConnParams, uuid string, timeout time.Duration, expectStates ...string) string {
	query, err := sqlparser.ParseAndBind("select state from mysql.vreplication where workflow=%a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statesMap := map[string]bool{}
	for _, state := range expectStates {
		statesMap[string(state)] = true
	}
	startTime := time.Now()
	lastKnownVreplicationState := ""
	for time.Since(startTime) < timeout {
		r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			lastKnownVreplicationState = row["state"].ToString()
			fmt.Printf("lastKnownVreplicationState: %s\n", lastKnownVreplicationState)

			if statesMap[lastKnownVreplicationState] {
				return lastKnownVreplicationState
			}
		}
		time.Sleep(1 * time.Second)
	}
	return lastKnownVreplicationState
}

func testPause(t *testing.T) {
	defer cluster.PanicHandler(t)
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	ddlStrategy := "vitess"

	createParams := func(ddlStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) *testOnlineDDLStatementParams {
		return &testOnlineDDLStatementParams{
			ddlStatement: ddlStatement,
			ddlStrategy:  ddlStrategy,
			expectHint:   expectHint,
			expectError:  expectError,
			skipWait:     skipWait,
		}
	}

	var (
		t1uuid string
		t2uuid string

		t1Name = "pause_test1"
		t2Name = "pause_test2"

		createT1Statement = `
			CREATE TABLE pause_test1 (
				id INT AUTO_INCREMENT PRIMARY KEY,
				hint_col varchar(64) not null default 'just-created'
			) ENGINE=InnoDB
		`
		createT2Statement = `
			CREATE TABLE pause_test2 (
				id INT AUTO_INCREMENT PRIMARY KEY,
				hint_col varchar(64) not null default 'just-created'
			) ENGINE=InnoDB
		`
		InsertT1Statement = `
			INSERT INTO pause_test1 (hint_col) VALUES
		  		('newborn22'),
		  		('newborn66');
		`
		InsertSelectT1Statement = `
			INSERT INTO pause_test1 (hint_col)
			SELECT hint_col
			FROM pause_test1;
		`
		InsertT2Statement = `
			INSERT INTO pause_test2 (hint_col) VALUES
		  		('newborn22'),
		  		('newborn66');
		`
		InsertSelectT2Statement = `
			INSERT INTO pause_test2 (hint_col)
			SELECT hint_col
			FROM pause_test2;
		`
		AlterT1StatementAddCol1 = `
			ALTER TABLE pause_test1 add column new_col11 int;
		`
		AlterT1StatementAddCol2 = `
			ALTER TABLE pause_test1 add column new_col12 int;
		`
		AlterT1StatementAddCol3 = `
			ALTER TABLE pause_test1 add column new_col13 int;
		`
		AlterT1StatementAddCol4 = `
			ALTER TABLE pause_test1 add column new_col14 int;
		`
		AlterT1StatementDropCol1 = `
			ALTER TABLE pause_test1 drop column new_col11;
		`
		AlterT1StatementDropCol2 = `
			ALTER TABLE pause_test1 drop column new_col12;
			`
		AlterT1StatementDropCol3 = `
			ALTER TABLE pause_test1 drop column new_col13;
		`
		AlterT1StatementDropCol4 = `
			ALTER TABLE pause_test1 drop column new_col14;
		`

		AlterT2StatementAddCol1 = `
			ALTER TABLE pause_test2 add column new_col21 int;
		`
		AlterT2StatementAddCol2 = `
			ALTER TABLE pause_test2 add column new_col22 int;
		`
		AlterT2StatementAddCol3 = `
			ALTER TABLE pause_test2 add column new_col23 int;
		`
		AlterT2StatementDropCol1 = `
			ALTER TABLE pause_test2 drop column new_col21;
		`
		AlterT2StatementDropCol2 = `
			ALTER TABLE pause_test2 drop column new_col22;
		`
		AlterT2StatementDropCol3 = `
			ALTER TABLE pause_test2 drop column new_col23;
		`

		PauseOnlineDDLStatement = `
			ALTER vitess_migration '%s' pause;
		`
		ResumeOnlineDDLStatement = `
			ALTER vitess_migration '%s' resume;
		`
		PauseAllOnlineDDLStatement = `
			ALTER vitess_migration pause all;
		`
		ResumeAllOnlineDDLStatement = `
			ALTER vitess_migration resume all;
		`
		LaunchOnlineDDLStatement = `
			ALTER vitess_migration '%s' launch;
		`
		CompleteOnlineDDLStatement = `
			ALTER vitess_migration '%s' complete;
		`
	)

	testReadTimestamp := func(t *testing.T, uuid string, timestampColumn string) (timestamp string) {
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			timestamp = row.AsString(timestampColumn, "")
			assert.NotEmpty(t, timestamp)
		}
		return timestamp
	}
	testTableSequentialTimes := func(t *testing.T, firstUUID, secondUUID string) {
		// expect secondUUID to start after firstUUID completes
		t.Run("Compare t1, t2 sequential times", func(t *testing.T) {
			endTime1 := testReadTimestamp(t, firstUUID, "completed_timestamp")
			startTime2 := testReadTimestamp(t, secondUUID, "started_timestamp")
			assert.GreaterOrEqual(t, startTime2, endTime1)
		})
	}

	t.Run("CREATE TABLEs", func(t *testing.T) {
		{ // The table pause_test does not exist
			t1uuid = testOnlineDDLStatement(t, createParams(createT1Statement, ddlStrategy, "vtgate", "", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		}
		{
			// The table pause_test2 does not exist
			t2uuid = testOnlineDDLStatement(t, createParams(createT2Statement, ddlStrategy, "vtgate", "just-created", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t2Name, true)
		}
	})

	t.Run("INSERT DATA", func(t *testing.T) {
		{
			onlineddl.VtgateExecQuery(t, &vtParams, InsertT1Statement, "")
			for i := 0; i < 19; i++ {
				onlineddl.VtgateExecQuery(t, &vtParams, InsertSelectT1Statement, "")
			}
			testRows(t, t1Name, int64(math.Pow(2, 20)))
		}
		{
			onlineddl.VtgateExecQuery(t, &vtParams, InsertT2Statement, "")
			for i := 0; i < 19; i++ {
				onlineddl.VtgateExecQuery(t, &vtParams, InsertSelectT2Statement, "")
			}
			testRows(t, t2Name, int64(math.Pow(2, 20)))
		}
	})

	// basic test of pause and resume
	t.Run("basic pause and resume test", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementAddCol1, ddlStrategy, "vtgate", "", "", true))
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)

		uuid2 := testOnlineDDLStatement(t, createParams(AlterT2StatementAddCol1, ddlStrategy, "vtgate", "", "", false))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid2, schema.OnlineDDLStatusComplete)

		uuid3 := testOnlineDDLStatement(t, createParams(AlterT1StatementAddCol2, ddlStrategy, "vtgate", "", "", true))

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")

		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid3, 10*time.Minute, schema.OnlineDDLStatusComplete))

		testTableSequentialTimes(t, uuid1, uuid3)

		checkTableColExist(t, t1Name, "new_col11")
		checkTableColExist(t, t1Name, "new_col12")
		checkTableColExist(t, t2Name, "new_col21")

	})

	// basic test of pause all and resume all
	t.Run("basic pause all and resume all test", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementAddCol3, ddlStrategy, "vtgate", "", "", true))
		uuid2 := testOnlineDDLStatement(t, createParams(AlterT2StatementAddCol2, ddlStrategy, "vtgate", "", "", true))
		uuid3 := testOnlineDDLStatement(t, createParams(AlterT2StatementAddCol3, ddlStrategy, "vtgate", "", "", true))

		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning)

		onlineddl.VtgateExecQuery(t, &vtParams, PauseAllOnlineDDLStatement, "")

		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid2, schema.OnlineDDLStatusPaused)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid3, schema.OnlineDDLStatusPaused)

		onlineddl.VtgateExecQuery(t, &vtParams, ResumeAllOnlineDDLStatement, "")

		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid2, 10*time.Minute, schema.OnlineDDLStatusComplete))
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid3, 10*time.Minute, schema.OnlineDDLStatusComplete))

		// because pause all is simply pause ddl task one by one, so the following situation is possible to happen:
		// 1. uuid1 is paused when ready
		// 2. after uuid1 is paused, uuid2 is scheduled by onlineDDL scheduler and start running, then is paused when running
		// 3. when resume all, uuid2 run ahead uuid1
		testTableSequentialTimes(t, uuid2, uuid3)

		checkTableColExist(t, t1Name, "new_col13")
		checkTableColExist(t, t2Name, "new_col22")
		checkTableColExist(t, t2Name, "new_col23")

	})

	// test of artifacts
	t.Run("artifacts", func(t *testing.T) {
		// artifacts should remain unchanged regardless of how many times "pause" and "resume"
		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementAddCol4, ddlStrategy, "vtgate", "", "", true))

		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning)
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")

		artifacts1 := getArtifactsOfMigration(t, uuid1)
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning)
		// ensure no extra shadow tables are added
		checkArtifactsOfMigration(t, uuid1, artifacts1)

		// pause second times
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)
		// resume again
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning)
		checkArtifactsOfMigration(t, uuid1, artifacts1)

		uuid2 := testOnlineDDLStatement(t, createParams(AlterT1StatementDropCol4, ddlStrategy, "vtgate", "", "", true))
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid2, 10*time.Minute, schema.OnlineDDLStatusComplete)
		checkTableColNotExist(t, t1Name, "new_col14")
	})

	// test of --postpone-launch, resume before launch
	t.Run("postpone launch, resume before launch", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementDropCol1, ddlStrategy+" --postpone-launch", "vtgate", "", "", true))
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)
		checkStateBeforePauseOfMigration(t, uuid1, "queued")

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusQueued)
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(LaunchOnlineDDLStatement, uuid1), "")

		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t1Name, "new_col11")
	})

	// test of --postpone-launch, launch before resume
	t.Run("postpone launch, launch before resume", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT2StatementDropCol1, ddlStrategy+" --postpone-launch", "vtgate", "", "", true))
		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)
		checkStateBeforePauseOfMigration(t, uuid1, "queued")

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(LaunchOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")

		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t2Name, "new_col21")
	})

	// test of --postpone-completion, resume before complete
	t.Run("postpone completion, resume before complete", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementDropCol2, ddlStrategy+" --postpone-completion", "vtgate", "", "", true))
		assert.Equal(t, schema.OnlineDDLStatusRunning, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning))

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)
		checkStateBeforePauseOfMigration(t, uuid1, "running")

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")
		assert.Equal(t, schema.OnlineDDLStatusRunning, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning))

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(CompleteOnlineDDLStatement, uuid1), "")
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t1Name, "new_col12")
	})

	// test of --postpone-completion, complete before resume
	t.Run("postpone completion, complete before resume", func(t *testing.T) {

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT2StatementDropCol2, ddlStrategy+" --postpone-completion", "vtgate", "", "", true))
		assert.Equal(t, schema.OnlineDDLStatusRunning, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusRunning))

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		checkStateBeforePauseOfMigration(t, uuid1, "running")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(CompleteOnlineDDLStatement, uuid1), "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusPaused)

		onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(ResumeOnlineDDLStatement, uuid1), "")

		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t2Name, "new_col22")
	})

	// when onlineDDL is ready to cut over or is doing cut-over, it can not be paused
	t.Run("can not pause after cutover starts, want to pause just before stop vreplication", func(t *testing.T) {
		// want to pause just before stop vreplication, and it will not succeed

		// enable failpoint, and cutover will sleep 1 minite just before set vreplication state to "stopped"
		onlineddl.VtgateExecQuery(t, &vtParams, "set @put_failpoint='vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustBeforeStopVreplication=return(true)';", "")

		r := onlineddl.VtgateExecQuery(t, &vtParams, "show failpoints;", "")
		for _, row := range r.Named().Rows {
			fmt.Printf("%s	%s\n", row["failpoint keys"], row["Enabled"])
		}

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT1StatementDropCol3, ddlStrategy, "vtgate", "", "", true))
		// because of the failpoint, and cutover will sleep 1 minite while the migration stage is "stopping vreplication"
		assert.Equal(t, "stopping vreplication", onlineddl.WaitForMigrationStage(t, &vtParams, shards, uuid1, 10*time.Minute, "stopping vreplication"))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusRunning)

		// pause will wait, because it will acquire mutex which cutover holds
		pauseRst := onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		assert.Equal(t, 0, int(pauseRst.RowsAffected))

		onlineddl.VtgateExecQuery(t, &vtParams, "set @remove_failpoint='vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustBeforeStopVreplication';", "")
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t1Name, "new_col13")
	})

	// when onlineDDL is ready to cut over or is doing cut-over, it can not be paused
	t.Run("can not pause after cutover starts, want to pause just after stop vreplication", func(t *testing.T) {
		// want to pause just after stop vreplication, and it will not succeed

		// enable failpoint, and cutover will sleep 1 minite just before set vreplication state to "stopped"
		onlineddl.VtgateExecQuery(t, &vtParams, "set @put_failpoint='vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustAfterStopVreplication=return(true)';", "")

		r := onlineddl.VtgateExecQuery(t, &vtParams, "show failpoints;", "")
		for _, row := range r.Named().Rows {
			fmt.Printf("%s	%s\n", row["failpoint keys"], row["Enabled"])
		}

		uuid1 := testOnlineDDLStatement(t, createParams(AlterT2StatementDropCol3, ddlStrategy, "vtgate", "", "", true))
		// because of the failpoint, and cutover will sleep 1 minite after the vreplication state is "Stopped"
		assert.Equal(t, "Stopped", WaitForVreplicationState(t, &vtParams, uuid1, 10*time.Minute, "Stopped"))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid1, schema.OnlineDDLStatusRunning)

		// pause will wait, because it will acquire mutex which cutover holds
		pauseRst := onlineddl.VtgateExecQuery(t, &vtParams, fmt.Sprintf(PauseOnlineDDLStatement, uuid1), "")
		assert.Equal(t, 0, int(pauseRst.RowsAffected))

		onlineddl.VtgateExecQuery(t, &vtParams, "set @remove_failpoint='vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustAfterStopVreplication';", "")
		assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid1, 10*time.Minute, schema.OnlineDDLStatusComplete))

		checkTableColNotExist(t, t2Name, "new_col23")
	})

}

func testScheduler(t *testing.T) {
	defer cluster.PanicHandler(t)
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	ddlStrategy := "vitess"

	createParams := func(ddlStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) *testOnlineDDLStatementParams {
		return &testOnlineDDLStatementParams{
			ddlStatement: ddlStatement,
			ddlStrategy:  ddlStrategy,
			expectHint:   expectHint,
			expectError:  expectError,
			skipWait:     skipWait,
		}
	}

	createRevertParams := func(revertUUID string, ddlStrategy string, executeStrategy string, expectError string, skipWait bool) *testRevertMigrationParams {
		return &testRevertMigrationParams{
			revertUUID:      revertUUID,
			executeStrategy: executeStrategy,
			ddlStrategy:     ddlStrategy,
			expectError:     expectError,
			skipWait:        skipWait,
		}
	}

	mysqlVersion := onlineddl.GetMySQLVersion(t, clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet())
	require.NotEmpty(t, mysqlVersion)
	_, capableOf, _ := mysql.GetFlavor(mysqlVersion, nil)

	var (
		t1uuid string
		t2uuid string

		t1Name = "t1_test"
		t2Name = "t2_test"

		createT1Statement = `
			CREATE TABLE t1_test (
				id bigint(20) not null,
				hint_col varchar(64) not null default 'just-created',
				PRIMARY KEY (id)
			) ENGINE=InnoDB
		`
		createT2Statement = `
			CREATE TABLE t2_test (
				id bigint(20) not null,
				hint_col varchar(64) not null default 'just-created',
				PRIMARY KEY (id)
			) ENGINE=InnoDB
		`
		createT1IfNotExistsStatement = `
			CREATE TABLE IF NOT EXISTS t1_test (
				id bigint(20) not null,
				hint_col varchar(64) not null default 'should_not_appear',
				PRIMARY KEY (id)
			) ENGINE=InnoDB
		`
		trivialAlterT1Statement = `
			ALTER TABLE t1_test ENGINE=InnoDB;
		`
		trivialAlterT2Statement = `
			ALTER TABLE t2_test ENGINE=InnoDB;
		`
		trivialAlterT3Statement = `
			ALTER TABLE t3_test ENGINE=InnoDB;
		`
		instantAlterT1Statement = `
			ALTER TABLE t1_test ADD COLUMN i0 INT NOT NULL DEFAULT 0;
		`
		dropT1Statement = `
			DROP TABLE IF EXISTS t1_test
		`
		dropT3Statement = `
			DROP TABLE IF EXISTS t3_test
		`
		dropT4Statement = `
			DROP TABLE IF EXISTS t4_test
		`
	)

	testReadTimestamp := func(t *testing.T, uuid string, timestampColumn string) (timestamp string) {
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			timestamp = row.AsString(timestampColumn, "")
			assert.NotEmpty(t, timestamp)
		}
		return timestamp
	}
	testTableSequentialTimes := func(t *testing.T, uuid1, uuid2 string) {
		// expect uuid2 to start after uuid1 completes
		t.Run("Compare t1, t2 sequential times", func(t *testing.T) {
			endTime1 := testReadTimestamp(t, uuid1, "completed_timestamp")
			startTime2 := testReadTimestamp(t, uuid2, "started_timestamp")
			assert.GreaterOrEqual(t, startTime2, endTime1)
		})
	}
	testTableCompletionTimes := func(t *testing.T, uuid1, uuid2 string) {
		// expect uuid1 to complete before uuid2
		t.Run("Compare t1, t2 completion times", func(t *testing.T) {
			endTime1 := testReadTimestamp(t, uuid1, "completed_timestamp")
			endTime2 := testReadTimestamp(t, uuid2, "completed_timestamp")
			assert.GreaterOrEqual(t, endTime2, endTime1)
		})
	}
	testAllowConcurrent := func(t *testing.T, name string, uuid string, expect int64) {
		t.Run("verify allow_concurrent: "+name, func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				allowConcurrent := row.AsInt64("allow_concurrent", 0)
				assert.Equal(t, expect, allowConcurrent)
			}
		})
	}

	// CREATE
	t.Run("CREATE TABLEs t1, t1", func(t *testing.T) {
		{ // The table does not exist
			t1uuid = testOnlineDDLStatement(t, createParams(createT1Statement, ddlStrategy, "vtgate", "just-created", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		}
		{
			// The table does not exist
			t2uuid = testOnlineDDLStatement(t, createParams(createT2Statement, ddlStrategy, "vtgate", "just-created", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t2Name, true)
		}
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})
	t.Run("Postpone launch CREATE", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(createT1IfNotExistsStatement, ddlStrategy+" --postpone-launch", "vtgate", "", "", true)) // skip wait
		time.Sleep(2 * time.Second)
		rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			postponeLaunch := row.AsInt64("postpone_launch", 0)
			assert.Equal(t, int64(1), postponeLaunch)
		}
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)

		t.Run("launch all shards", func(t *testing.T) {
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, t1uuid, "", true)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(0), postponeLaunch)
			}
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("Postpone launch ALTER", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" --postpone-launch", "vtgate", "", "", true)) // skip wait
		time.Sleep(2 * time.Second)
		rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			postponeLaunch := row.AsInt64("postpone_launch", 0)
			assert.Equal(t, int64(1), postponeLaunch)
		}
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)

		t.Run("launch irrelevant UUID", func(t *testing.T) {
			someOtherUUID := "00000000_1111_2222_3333_444444444444"
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, someOtherUUID, "", false)
			time.Sleep(2 * time.Second)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(1), postponeLaunch)
			}
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)
		})
	})
	t.Run("ALTER both tables non-concurrent", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait

		t.Run("wait for t1 complete", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		})
		t.Run("wait for t1 complete", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		})
		t.Run("check both complete", func(t *testing.T) {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})
	t.Run("ALTER both tables non-concurrent, postponed", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 0)
		t.Run("expect t1 running, t2 queued", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			// now that t1 is running, let's unblock t2. We expect it to remain queued.
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			time.Sleep(ensureStateNotChangedTime)
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// non-concurrent -- should be queued!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("complete t1", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("expect t2 to complete", func(t *testing.T) {
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})

	t.Run("ALTER both tables, elligible for concurrenct", func(t *testing.T) {
		// ALTER TABLE is allowed to run concurrently when no other ALTER is busy with copy state. Our tables are tiny so we expect to find both migrations running
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" --allow-concurrent --postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" --allow-concurrent --postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "t2", t2uuid, 1)
		t.Run("expect both running", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusRunning)
		})
		t.Run("complete t2", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should still be running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		testTableCompletionTimes(t, t2uuid, t1uuid)
	})
	t.Run("ALTER both tables, elligible for concurrenct, with throttling", func(t *testing.T) {
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)
		// ALTER TABLE is allowed to run concurrently when no other ALTER is busy with copy state. Our tables are tiny so we expect to find both migrations running
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "t2", t2uuid, 1)
		t.Run("expect t1 running", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			// since all migrations are throttled, t1 migration is not ready_to_complete, hence
			// t2 should not be running
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("unthrottle, expect t2 running", func(t *testing.T) {
			onlineddl.UnthrottleAllMigrations(t, &vtParams)
			// t1 should now be ready_to_complete, hence t2 should start running
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, extendedWaitTime, schema.OnlineDDLStatusRunning)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusRunning)
		})
		t.Run("complete t2", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should still be running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		testTableCompletionTimes(t, t2uuid, t1uuid)
	})
	t.Run("REVERT both tables concurrent, postponed", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))
		t2uuid = testRevertMigration(t, createRevertParams(t2uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))

		testAllowConcurrent(t, "t1", t1uuid, 1)
		t.Run("expect both migrations to run", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
		})
		t.Run("test ready-to-complete", func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		t.Run("complete t2", func(t *testing.T) {
			// now that both are running, let's unblock t2. We expect it to complete.
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("concurrent REVERT vs two non-concurrent DROPs", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))
		drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "drop3", drop3uuid, 0)
		t.Run("expect t1 migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait
		t.Run("drop3 complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("drop1 blocked", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, drop1uuid, true)
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusCancelled)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})

	t.Run("non-concurrent REVERT vs three concurrent drops", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -postpone-completion", "vtgate", "", true))
		drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true))                      // skip wait
		drop4uuid := testOnlineDDLStatement(t, createParams(dropT4Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "drop3", drop3uuid, 1)
		t.Run("expect t1 migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true)) // skip wait
		testAllowConcurrent(t, "drop1", drop1uuid, 1)
		t.Run("t3drop complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("t1drop blocked", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("t4 postponed", func(t *testing.T) {
			// drop4 migration should postpone.
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop4uuid, schema.OnlineDDLStatusQueued)
			// Issue a complete and wait for successful completion. drop4 is non-conflicting and should be able to proceed
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, drop4uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop4uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop4uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("t1drop unblocked", func(t *testing.T) {
			// t1drop should now be unblocked!
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, false)
		})
		t.Run("revert t1 drop", func(t *testing.T) {
			revertDrop3uuid := testRevertMigration(t, createRevertParams(drop1uuid, ddlStrategy+" -allow-concurrent", "vtgate", "", true))
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertDrop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertDrop3uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		})
	})
	t.Run("conflicting migration does not block other queued migrations", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy, "vtgate", "", "", false)) // skip wait
		t.Run("trivial t1 migration", func(t *testing.T) {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		})

		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -postpone-completion", "vtgate", "", true))
		t.Run("expect t1 revert migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true)) // skip wait
		t.Run("t1drop blocked", func(t *testing.T) {
			time.Sleep(ensureStateNotChangedTime)
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusReady)
		})
		t.Run("t3 ready to complete", func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, drop1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		t.Run("t3drop complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			// even though t1drop is blocked! This is the heart of this test
			drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("cancel drop1", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusReady)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, drop1uuid, true)
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusCancelled)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})

	// INSTANT DDL
	instantDDLCapable, err := capableOf(mysql.InstantAddLastColumnFlavorCapability)
	require.NoError(t, err)
	t.Logf("instantDDLCapable : %v", instantDDLCapable)
	if instantDDLCapable {
		t.Run("INSTANT DDL: postpone-completion", func(t *testing.T) {
			t1uuid := testOnlineDDLStatement(t, createParams(instantAlterT1Statement, ddlStrategy+" --prefer-instant-ddl --postpone-completion", "vtgate", "", "", true))

			t.Run("expect t1 queued", func(t *testing.T) {
				// we want to validate that the migration remains queued even after some time passes. It must not move beyond 'queued'
				time.Sleep(ensureStateNotChangedTime)
				onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			})
			t.Run("complete t1", func(t *testing.T) {
				// Issue a complete and wait for successful completion
				onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			})
		})
	}
	// in-order-completion
	t.Run("alter non-exist table before ", func(t *testing.T) {
		t.Run("alter non-exist table, it should be fail", func(t *testing.T) {
			dropUUID := testOnlineDDLStatement(t, createParams(trivialAlterT3Statement, "online", "vtgate", "", "", false))
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, dropUUID, normalWaitTime, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, dropUUID, schema.OnlineDDLStatusFailed)
		})
		t.Run("alter exist table, it should be completed", func(t *testing.T) {
			dropUUID := testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, "online", "vtgate", "", "", false))
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, dropUUID, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, dropUUID, schema.OnlineDDLStatusComplete)
		})
	})
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, params *testOnlineDDLStatementParams) (uuid string) {
	strategySetting, err := schema.ParseDDLStrategy(params.ddlStrategy)
	require.NoError(t, err)

	tableName := parseTableName(t, params.ddlStatement)

	require.Empty(t, params.migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
	result := onlineddl.VtgateExecDDL(t, &vtParams, params.ddlStrategy, params.ddlStatement, params.expectError)
	if result != nil {
		row := result.Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	}

	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	if !strategySetting.Strategy.IsDirect() && !params.skipWait && uuid != "" {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if params.expectError == "" && params.expectHint != "" {
		checkMigratedTable(t, tableName, params.expectHint)
	}
	return uuid
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, params *testRevertMigrationParams) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", params.revertUUID)
	if params.executeStrategy == "vtgate" {
		require.Empty(t, params.migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
		result := onlineddl.VtgateExecDDL(t, &vtParams, params.ddlStrategy, revertQuery, params.expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, revertQuery, cluster.VtctlClientParams{DDLStrategy: params.ddlStrategy, MigrationContext: params.migrationContext, SkipPreflight: true})
		if params.expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, params.expectError)
		}
	}

	if params.expectError == "" {
		uuid = strings.TrimSpace(uuid)
		fmt.Println("# Generated UUID (for debug purposes):")
		fmt.Printf("<%s>\n", uuid)
	}
	if !params.skipWait {
		time.Sleep(time.Second * 20)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t *testing.T, showTableName string, expectExists bool) bool {
	expectCount := 0
	if expectExists {
		expectCount = 1
	}
	for i := range clusterInstance.Keyspaces[0].Shards {
		if !checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, expectCount) {
			return false
		}
	}
	return true
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) bool {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.VttabletProcess.QueryTablet(query, vtParams.DbName, true)
	require.Nil(t, err)
	return assert.Equal(t, expectCount, len(queryResult.Rows))
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectHint string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectHint)
	}
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), vtParams.DbName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.GreaterOrEqual(t, len(queryResult.Rows[0]), 2) // table name, create statement, and if it's a view then additional columns
	statement = queryResult.Rows[0][1].ToString()
	return statement
}
