/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func buildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: collations.CollationUtf8ID,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

func buildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

func (jc *JobController) buildJobSubmitResult(jobUUID, jobBatchTable string, timeGap, subtaskRows int64, postponeLaunch bool, failPolicy string) *sqltypes.Result {
	var rows []sqltypes.Row
	row := buildVarCharRow(jobUUID, jobBatchTable, strconv.FormatInt(timeGap, 10), strconv.FormatInt(subtaskRows, 10), failPolicy, strconv.FormatBool(postponeLaunch))
	rows = append(rows, row)
	submitRst := &sqltypes.Result{
		Fields:       buildVarCharFields("job_uuid", "batch_info_table_name", "time_gap_in_ms", "batch_size", "fail_policy", "postpone_launch"),
		Rows:         rows,
		RowsAffected: 1,
	}
	return submitRst
}

// execQuery execute sql by using connect poll,so if targetString is not empty, it will add prefix `use database` first then execute sql.
func (jc *JobController) execQuery(ctx context.Context, targetString, query string) (result *sqltypes.Result, err error) {
	defer jc.env.LogError()
	var setting pools.Setting
	if targetString != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", targetString))
		setting.SetResetQuery(fmt.Sprintf("use %s", jc.env.Config().DB.DBName))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	if err != nil {
		return result, err
	}
	qr, err := conn.Exec(ctx, query, math.MaxInt32, true)
	conn.Recycle()
	return qr, err

}

// 该函数拿锁
func (jc *JobController) updateJobMessage(ctx context.Context, uuid, message string) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateMessage,
		sqltypes.StringBindVariable(message),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	return err
}

func (jc *JobController) updateJobAffectedRows(ctx context.Context, uuid string, affectedRows int64) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateAffectedRows,
		sqltypes.Int64BindVariable(affectedRows),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	return err
}

func (jc *JobController) updateJobStatus(ctx context.Context, uuid, status, statusSetTime string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
		sqltypes.StringBindVariable(status),
		sqltypes.StringBindVariable(statusSetTime),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) updateJobPeriodTime(ctx context.Context, uuid, timePeriodStart, timePeriodEnd string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateTimePeriod,
		sqltypes.StringBindVariable(timePeriodStart),
		sqltypes.StringBindVariable(timePeriodEnd),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) getIntJobInfo(ctx context.Context, uuid, fieldName string) (int64, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobGetInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return 0, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, fmt.Errorf("uuid %s has %d entrys in the table instead of 1", uuid, len(qr.Named().Rows))
	}
	return qr.Named().Rows[0].ToInt64(fieldName)
}

func (jc *JobController) getStrJobInfo(ctx context.Context, uuid, fieldName string) (string, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobGetInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return "", err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", fmt.Errorf("uuid %s has %d entrys in the table instead of 1", uuid, len(qr.Named().Rows))
	}
	return qr.Named().Rows[0].ToString(fieldName)
}

func (jc *JobController) getBatchIDToExec(ctx context.Context, batchTableSchema, batchTableName string) (string, error) {
	getBatchIDToExecSQL := fmt.Sprintf(sqlTemplateGetBatchIDToExec, batchTableName)
	qr, err := jc.execQuery(ctx, batchTableSchema, getBatchIDToExecSQL)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", nil
	}
	return qr.Named().Rows[0].ToString("batch_id")
}

func (jc *JobController) updateDealingBatchID(ctx context.Context, uuid string, batchID string) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlUpdateDealingBatchID,
		sqltypes.StringBindVariable(batchID),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return err
	}
	return nil
}

// todo feat batch的并行执行，注意对batch表的操作
func (jc *JobController) getBatchSQLsByID(ctx context.Context, batchID, batchTableName, tableSchema string) (batchSQL, batchCountSQL string, err error) {
	getBatchSQLWithTableName := fmt.Sprintf(sqlTemplateGetBatchSQLsByID, batchTableName)
	query, err := sqlparser.ParseAndBind(getBatchSQLWithTableName,
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return "", "", err
	}
	qr, err := jc.execQuery(ctx, tableSchema, query)
	if err != nil {
		return "", "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", "", errors.New("the len of qr of getting batch sql by ID is not 1")
	}
	batchSQL, _ = qr.Named().Rows[0].ToString("batch_sql")
	batchCountSQL, _ = qr.Named().Rows[0].ToString("batch_count_sql_when_creating_batch")
	return batchSQL, batchCountSQL, nil
}

func (jc *JobController) getMaxBatchID(ctx context.Context, batchTableName, tableSchema string) (string, error) {
	getMaxBatchIDWithTableName := fmt.Sprintf(sqlTemplateGetMaxBatchID, batchTableName)
	qr, err := jc.execQuery(ctx, tableSchema, getMaxBatchIDWithTableName)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", errors.New("the len of qr of getting batch sql by ID is not 1")
	}
	return qr.Named().Rows[0].ToString("max_batch_id")
}

func (jc *JobController) getTablePkInfo(ctx context.Context, tableSchema, tableName string) ([]PKInfo, error) {
	// 1. 先获取pks 的名字
	submitQuery, err := sqlparser.ParseAndBind(sqlGetTablePk,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return nil, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return nil, err
	}
	var pkNames []string
	for _, row := range qr.Named().Rows {
		pkNames = append(pkNames, row["COLUMN_NAME"].ToString())
	}

	// 2. 根据获得的pk列的名字，去原表中查一行数据，借助封装好的Value对象获得每个pk的类型
	pkCols := ""
	firstPK := true
	for _, pkName := range pkNames {
		if !firstPK {
			pkCols += ","
		}
		pkCols += pkName
		firstPK = false
	}
	selectPKCols := fmt.Sprintf(sqlTemplateSelectPKCols, pkCols, tableSchema, tableName)
	qr, err = jc.execQuery(ctx, "", selectPKCols)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) != 1 {
		return nil, errors.New("the len of qr of select pk cols should be 1")
	}
	// 获得每一列的type，并生成pkInfo切片
	var pkInfos []PKInfo
	for _, pkName := range pkNames {
		pkInfos = append(pkInfos, PKInfo{pkName: pkName, pkType: qr.Named().Rows[0][pkName].Type()})
	}

	return pkInfos, nil
}

func (jc *JobController) getTableColNames(ctx context.Context, tableSchema, tableName string) ([]string, error) {
	submitQuery, err := sqlparser.ParseAndBind(sqlGetTableColNames,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return nil, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return nil, err
	}
	var colNames []string
	for _, row := range qr.Named().Rows {
		colNames = append(colNames, row["COLUMN_NAME"].ToString())
	}
	return colNames, nil
}

func currentBatchIDInc(currentBatchID string) (string, error) {
	if strings.Contains(currentBatchID, "-") {
		currentBatchID = strings.Split(currentBatchID, "-")[0]
	}
	currentBatchIDInt64, err := strconv.ParseInt(currentBatchID, 10, 64)
	if err != nil {
		return "", err
	}
	currentBatchIDInt64++
	return strconv.FormatInt(currentBatchIDInt64, 10), nil
}

func (jc *JobController) getIndexCount(tableSchema, tableName string) (indexCount int64, err error) {
	query, err := sqlparser.ParseAndBind(sqlGetIndexCount,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	qr, err := jc.execQuery(ctx, "", query)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, err
	}
	return qr.Named().Rows[0]["index_count"].ToInt64()
}

func genNewBatchID(batchID string) (newBatchID string, err error) {
	// 产生新的batchID
	if strings.Contains(batchID, "-") {
		parts := strings.Split(batchID, "-")
		num, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return "", err
		}
		newBatchID = fmt.Sprintf("%s-%d", parts[0], num+1)
	} else {
		num, err := strconv.ParseInt(batchID, 10, 64)
		if err != nil {
			return "", err
		}
		newBatchID = fmt.Sprintf("%d-2", num)
	}
	return newBatchID, nil
}

func (jc *JobController) updateBatchStatus(batchTableSchema, batchTableName, status, batchID, errStr string) (err error) {
	updateBatchStatusAndAffectedRowsSQL := fmt.Sprintf(sqlTempalteUpdateBatchStatusAndAffectedRows, batchTableName)
	query, err := sqlparser.ParseAndBind(updateBatchStatusAndAffectedRowsSQL,
		sqltypes.StringBindVariable(status+": "+errStr),
		sqltypes.Int64BindVariable(0),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(context.Background(), batchTableSchema, query)
	return err
}
