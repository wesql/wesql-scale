package engine

import (
	"context"
	"fmt"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/branch"
)

type VTGateMysqlService struct {
	VCursor VCursor
}

func (v *VTGateMysqlService) Query(query string) (branch.Rows, error) {
	// AUTOCOMMIT is used to run the statement as autocommitted transaction.
	// AUTOCOMMIT = 3;
	rst, err := v.VCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 3)
	if err != nil {
		return nil, err
	}

	var results branch.Rows = make([]branch.Row, 0)

	for _, r := range rst.Rows {
		if len(rst.Fields) != len(r) {
			return nil, fmt.Errorf("field length not equal to row value length")
		}
		rowMap := make(map[string]branch.Bytes)
		for i, v := range r {
			bytes, err := v.ToBytes()
			if err != nil {
				return nil, err
			}
			rowMap[rst.Fields[i].Name] = bytes
		}
		results = append(results, branch.Row{RowData: rowMap})
	}

	return results, nil
}

func (v *VTGateMysqlService) Exec(database, query string) (*branch.Result, error) {
	if database != "" {
		err := v.VCursor.Session().SetTarget(database)
		if err != nil {
			return nil, err
		}
	}

	rst, err := v.VCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 3)
	if err != nil {
		return nil, err
	}

	return &branch.Result{AffectedRows: rst.RowsAffected, LastInsertID: rst.InsertID}, nil
}

func (v *VTGateMysqlService) ExecuteInTxn(queries ...string) error {
	first := true
	defer v.VCursor.Execute(context.Background(), "Execute", "ROLLBACK;", make(map[string]*querypb.BindVariable), true, 0)
	for _, query := range queries {
		if first {
			_, err := v.VCursor.Execute(context.Background(), "Execute", "start transaction;", make(map[string]*querypb.BindVariable), true, 0)
			if err != nil {
				return err
			}
			first = false
		}
		// NORMAL is the default commit order.
		// NORMAL = 0;
		_, err := v.VCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 0)
		if err != nil {
			return err
		}
	}

	_, err := v.VCursor.Execute(context.Background(), "Execute", "COMMIT;", make(map[string]*querypb.BindVariable), true, 0)
	return err
}
