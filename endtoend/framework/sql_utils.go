package framework

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func ExecuteSqlScript(db *sql.DB, sqlScript string) error {
	_, err := db.Exec(sqlScript)
	return err
}

func ExecNoError(t *testing.T, db *sql.DB, sql string, args ...any) {
	t.Helper()
	fmt.Println(sql)
	_, err := db.Exec(sql, args...)
	assert.NoError(t, err)
}

func QueryNoError(t *testing.T, db *sql.DB, sql string, args ...any) *sql.Rows {
	t.Helper()
	fmt.Println(sql)
	rows, err := db.Query(sql, args...)
	assert.NoError(t, err)
	return rows
}

func ExecWithErrorContains(t *testing.T, db *sql.DB, contains string, sql string, args ...any) {
	t.Helper()
	fmt.Println(sql)
	_, err := db.Exec(sql, args...)
	assert.ErrorContains(t, err, contains)
}

func QueryWithErrorContains(t *testing.T, db *sql.DB, contains string, sql string, args ...any) {
	t.Helper()
	fmt.Println(sql)
	_, err := db.Query(sql, args...)
	assert.ErrorContains(t, err, contains)
}
