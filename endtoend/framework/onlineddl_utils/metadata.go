package onlineddl_utils

import "time"

const (
	OnlineDDLStatusRequested string = "requested"
	OnlineDDLStatusCancelled string = "cancelled"
	OnlineDDLStatusQueued    string = "queued"
	OnlineDDLStatusReady     string = "ready"
	OnlineDDLStatusRunning   string = "running"
	OnlineDDLStatusComplete  string = "complete"
	OnlineDDLStatusFailed    string = "failed"
	OnlineDDLStatusPaused    string = "paused"
)

type SchemaMigrationMetadata struct {
	ID                          int64
	MigrationUUID               string
	Keyspace                    string
	Shard                       string
	MySQLSchema                 string `db:"mysql_schema"`
	MySQLTable                  string `db:"mysql_table"`
	MigrationStatement          string
	Strategy                    string
	Options                     string
	AddedTimestamp              *time.Time
	RequestedTimestamp          *time.Time
	ReadyTimestamp              *time.Time
	StartedTimestamp            *time.Time
	LivenessTimestamp           *time.Time
	CompletedTimestamp          *time.Time
	CleanupTimestamp            *time.Time
	MigrationStatus             string
	StatusBeforePaused          *string
	LogPath                     string
	Artifacts                   string
	Retries                     int
	Tablet                      string
	TabletFailure               int
	Progress                    int
	MigrationContext            string
	DDLAction                   string `db:"ddl_action"`
	Message                     string
	ETASeconds                  int `db:"eta_seconds"`
	RowsCopied                  int64
	TableRows                   int64
	AddedUniqueKeys             int
	RemovedUniqueKeys           int
	LogFile                     string
	RetainArtifactsSeconds      int
	PostponeCompletion          int
	RemovedUniqueKeyNames       string
	DroppedNoDefaultColumnNames string
	ExpandedColumnNames         string
	RevertibleNotes             string
	AllowConcurrent             int
	RevertedUUID                string
	IsView                      int
	ReadyToComplete             int
	StowawayTable               string
	VitessLivenessIndicator     int
	UserThrottleRatio           float64
	SpecialPlan                 string
	LastThrottledTimestamp      *time.Time
	ComponentThrottled          string
	CancelledTimestamp          *time.Time
	PostponeLaunch              int
	Stage                       string
	CutoverAttempts             int
	IsImmediateOperation        int
	ReviewedTimestamp           *time.Time
}
