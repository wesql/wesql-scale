package onlineddl_utils

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func VtgateExecDDL(t *testing.T, db *sql.DB, ddlStrategy string, ddl string, expectError string) string {
	t.Helper()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	// Read original DDL strategy
	var originalStrategy string
	err = conn.QueryRowContext(ctx, "SELECT @@ddl_strategy").Scan(&originalStrategy)
	assert.NoError(t, err)

	// Set new DDL strategy
	_, err = conn.ExecContext(ctx, fmt.Sprintf("SET @@ddl_strategy='%s'", ddlStrategy))
	assert.NoError(t, err)

	// Ensure strategy is reset after execution
	defer func() {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("SET @@ddl_strategy='%s'", originalStrategy))
		assert.NoError(t, err)
	}()

	// Execute DDL
	var uuid string
	err = conn.QueryRowContext(ctx, ddl).Scan(&uuid)

	// Handle expected errors
	if expectError == "" {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectError)
	}

	return uuid
}

func GetSchemaMigrationMetadata(t *testing.T, db *sql.DB, uuid string) *SchemaMigrationMetadata {
	t.Helper()

	query := fmt.Sprintf("show vitess_migrations like '%s'", uuid)

	row := db.QueryRow(query)
	metadata := &SchemaMigrationMetadata{}

	err := row.Scan(
		&metadata.ID,
		&metadata.MigrationUUID,
		&metadata.Keyspace,
		&metadata.Shard,
		&metadata.MySQLSchema,
		&metadata.MySQLTable,
		&metadata.MigrationStatement,
		&metadata.Strategy,
		&metadata.Options,
		&metadata.AddedTimestamp,
		&metadata.RequestedTimestamp,
		&metadata.ReadyTimestamp,
		&metadata.StartedTimestamp,
		&metadata.LivenessTimestamp,
		&metadata.CompletedTimestamp,
		&metadata.CleanupTimestamp,
		&metadata.MigrationStatus,
		&metadata.StatusBeforePaused,
		&metadata.LogPath,
		&metadata.Artifacts,
		&metadata.Retries,
		&metadata.Tablet,
		&metadata.TabletFailure,
		&metadata.Progress,
		&metadata.MigrationContext,
		&metadata.DDLAction,
		&metadata.Message,
		&metadata.ETASeconds,
		&metadata.RowsCopied,
		&metadata.TableRows,
		&metadata.AddedUniqueKeys,
		&metadata.RemovedUniqueKeys,
		&metadata.LogFile,
		&metadata.RetainArtifactsSeconds,
		&metadata.PostponeCompletion,
		&metadata.RemovedUniqueKeyNames,
		&metadata.DroppedNoDefaultColumnNames,
		&metadata.ExpandedColumnNames,
		&metadata.RevertibleNotes,
		&metadata.AllowConcurrent,
		&metadata.RevertedUUID,
		&metadata.IsView,
		&metadata.ReadyToComplete,
		&metadata.StowawayTable,
		&metadata.VitessLivenessIndicator,
		&metadata.UserThrottleRatio,
		&metadata.SpecialPlan,
		&metadata.LastThrottledTimestamp,
		&metadata.ComponentThrottled,
		&metadata.CancelledTimestamp,
		&metadata.PostponeLaunch,
		&metadata.Stage,
		&metadata.CutoverAttempts,
		&metadata.IsImmediateOperation,
		&metadata.ReviewedTimestamp,
	)

	assert.NoError(t, err)
	return metadata
}
