package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRewriteTableName(t *testing.T) {
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]*querypb.BindVariable
	}{
		{
			in:      `select * from (select 12) as t`,
			outstmt: `select * from (select 12 from dual) as t`,
		},
		{
			in:      `select d1t1.*, d2t1.* from t1 as d1t1 join d2.t1 as d2t1;`,
			outstmt: `select d1t1.*, d2t1.* from test.t1 as d1t1 join d2.t1 as d2t1`,
		},
		{
			in:      `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1;`,
			outstmt: `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `select d1.t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt: `select d1.t1.*, t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `select t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt: `select t1.*, t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `with simple_query as (select * from t1) select * from simple_query;`,
			outstmt: `select t1.*, t1.* from d1.t1 join d2.t1`,
		},
		{
			in: `SELECT c.name, o.order_date, oi.product_name 
				FROM (   SELECT id, name   FROM customers ) c 
				JOIN (   SELECT id, customer_id, order_date   FROM orders ) o 
				ON c.id = o.customer_id 
				JOIN (   SELECT id, order_id, product_name   FROM order_items ) oi 
				ON o.id = oi.order_id 
				UNION 
				SELECT c.name, o.order_date, oi.product_name 
				FROM customers c 
				JOIN orders o 
				ON c.id = o.customer_id 
				JOIN order_items oi ON o.id = oi.order_id 
				WHERE oi.price > (   SELECT AVG(price)   FROM order_items );`,
			outstmt: "select c.`name`, o.order_date, oi.product_name " +
				"from (select id, `name` from test.customers) as c " +
				"join (select id, customer_id, order_date from test.orders) as o " +
				"on c.id = o.customer_id " +
				"join (select id, order_id, product_name from test.order_items) as oi " +
				"on o.id = oi.order_id " +
				"union " +
				"select c.`name`, o.order_date, oi.product_name " +
				"from test.customers as c " +
				"join test.orders as o " +
				"on c.id = o.customer_id " +
				"join test.order_items as oi on o.id = oi.order_id " +
				"where oi.price > (select avg(price) from test.order_items)",
			outbv: map[string]*querypb.BindVariable{},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)
			newStmt, _, err := RewriteTableName(stmt, "test")
			require.NoError(t, err)
			assert.Equal(t, tc.outstmt, String(newStmt))
		})
	}
}
