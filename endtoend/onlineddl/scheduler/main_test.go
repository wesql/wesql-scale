package scheduler

import (
	_ "embed"
	"flag"
	"github.com/wesql/wescale/endtoend/framework/clusters"
	"log"
	"os"
	"testing"
)

var dbName = "onlineddl_scheduler_e2e_test"

//go:embed setup.sql
var setupSql string

//go:embed cleanup.sql
var cleanupSql string

var cluster *clusters.SingleNodeCluster

func TestMain(m *testing.M) {
	cluster = clusters.NewDefaultSingleNodeCluster()
	cluster.RegisterFlagsForSingleNodeCluster()
	flag.Parse()

	// Setup the test environment
	err := cluster.SetUp(dbName, setupSql)
	if err != nil {
		log.Fatalf("Setup failed: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup the test environment
	err = cluster.CleanUp(cleanupSql)
	if err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}

	os.Exit(code)
}
