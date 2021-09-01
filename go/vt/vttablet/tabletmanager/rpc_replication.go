/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enableSemiSync   = flag.Bool("enable_semi_sync", false, "Enable semi-sync when configuring replication, on primary and replica tablets only (rdonly tablets will not ack).")
	setSuperReadOnly = flag.Bool("use_super_read_only", false, "Set super_read_only flag when performing planned failover.")
)

// ReplicationStatus returns the replication status
func (tm *TabletManager) ReplicationStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return nil, err
	}
	return mysql.ReplicationStatusToProto(status), nil
}

// MasterStatus is the old version of PrimaryStatus. Deprecated.
func (tm *TabletManager) MasterStatus(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	return tm.PrimaryStatus(ctx)
}

// PrimaryStatus returns the replication status for a primary tablet.
func (tm *TabletManager) PrimaryStatus(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	status, err := tm.MysqlDaemon.PrimaryStatus(ctx)
	if err != nil {
		return nil, err
	}
	return mysql.PrimaryStatusToProto(status), nil
}

// MasterPosition is the old version of PrimaryPosition. Deprecated.
func (tm *TabletManager) MasterPosition(ctx context.Context) (string, error) {
	return tm.PrimaryPosition(ctx)
}

// PrimaryPosition returns the position of a primary database
func (tm *TabletManager) PrimaryPosition(ctx context.Context) (string, error) {
	pos, err := tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// WaitForPosition waits until replication reaches the desired position
func (tm *TabletManager) WaitForPosition(ctx context.Context, pos string) error {
	log.Infof("WaitForPosition: %v", pos)
	mpos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}
	return tm.MysqlDaemon.WaitSourcePos(ctx, mpos)
}

// StopReplication will stop the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplication(ctx context.Context) error {
	log.Infof("StopReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.stopReplicationLocked(ctx)
}

func (tm *TabletManager) stopReplicationLocked(ctx context.Context) error {

	// Remember that we were told to stop, so we don't try to
	// restart ourselves (in replication_reporter).
	tm.replManager.setReplicationStopped(true)

	// Also tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to StopReplication"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	return tm.MysqlDaemon.StopReplication(tm.hookExtraEnv())
}

func (tm *TabletManager) stopIOThreadLocked(ctx context.Context) error {

	// Remember that we were told to stop, so we don't try to
	// restart ourselves (in replication_reporter).
	tm.replManager.setReplicationStopped(true)

	// Also tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to StopReplication"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	return tm.MysqlDaemon.StopIOThread(ctx)
}

// StopReplicationMinimum will stop the replication after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StopReplicationMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	log.Infof("StopReplicationMinimum: position: %v waitTime: %v", position, waitTime)
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	if err := tm.MysqlDaemon.WaitSourcePos(waitCtx, pos); err != nil {
		return "", err
	}
	if err := tm.stopReplicationLocked(ctx); err != nil {
		return "", err
	}
	pos, err = tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StartReplication will start the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (tm *TabletManager) StartReplication(ctx context.Context) error {
	log.Infof("StartReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tm.replManager.setReplicationStopped(false)

	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()

	if err := tm.fixSemiSync(tm.Tablet().Type); err != nil {
		return err
	}
	return tm.MysqlDaemon.StartReplication(tm.hookExtraEnv())
}

// StartReplicationUntilAfter will start the replication and let it catch up
// until and including the transactions in `position`
func (tm *TabletManager) StartReplicationUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	log.Infof("StartReplicationUntilAfter: position: %v waitTime: %v", position, waitTime)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	return tm.MysqlDaemon.StartReplicationUntilAfter(waitCtx, pos)
}

// GetReplicas returns the address of all the replicas
func (tm *TabletManager) GetReplicas(ctx context.Context) ([]string, error) {
	return mysqlctl.FindReplicas(tm.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (tm *TabletManager) ResetReplication(ctx context.Context) error {
	log.Infof("ResetReplication")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tm.replManager.setReplicationStopped(true)
	return tm.MysqlDaemon.ResetReplication(ctx)
}

// InitMaster is the old version of InitPrimary. Deprecated.
func (tm *TabletManager) InitMaster(ctx context.Context) (string, error) {
	return tm.InitPrimary(ctx)
}

// InitPrimary enables writes and returns the replication position.
func (tm *TabletManager) InitPrimary(ctx context.Context) (string, error) {
	log.Infof("InitPrimary")
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	// Initializing as primary implies undoing any previous "do not replicate".
	tm.replManager.setReplicationStopped(false)

	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := tm.MysqlDaemon.PrimaryPosition()
	if err != nil {
		return "", err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some replicas to be able to commit transactions.
	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite); err != nil {
		return "", err
	}

	// Enforce semi-sync after changing the tablet)type to PRIMARY. Otherwise, the
	// primary will hang while trying to create the database.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (tm *TabletManager) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, primaryAlias *topodatapb.TabletAlias, position string) error {
	log.Infof("PopulateReparentJournal: action: %v parent: %v  position: %v", actionName, primaryAlias, position)
	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(primaryAlias), pos))

	return tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds)
}

// InitReplica sets replication primary and position, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) InitReplica(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	log.Infof("InitReplica: parent: %v  position: %v", parent, position)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If we were a primary type, switch our type to replica.  This
	// is used on the old primary when using InitShardPrimary with
	// -force, and the new primary is different from the old primary.
	if tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_REPLICA, DBActionNone); err != nil {
			return err
		}
	}

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := tm.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	tm.replManager.setReplicationStopped(false)

	// If using semi-sync, we need to enable it before connecting to primary.
	// If we were a primary type, we need to switch back to replica settings.
	// Otherwise we won't be able to commit anything.
	tt := tm.Tablet().Type
	if tt == topodatapb.TabletType_PRIMARY {
		tt = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tt); err != nil {
		return err
	}

	if err := tm.MysqlDaemon.SetReplicationPosition(ctx, pos); err != nil {
		return err
	}
	if err := tm.MysqlDaemon.SetReplicationSource(ctx, ti.Tablet.MysqlHostname, int(ti.Tablet.MysqlPort), false /* stopReplicationBefore */, true /* stopReplicationAfter */); err != nil {
		return err
	}

	// wait until we get the replicated row, or our context times out
	return tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemotePrimary prepares a PRIMARY tablet to give up leadership to another tablet.
//
// It attemps to idempotently ensure the following guarantees upon returning
// successfully:
//   * No future writes will be accepted.
//   * No writes are in-flight.
//   * MySQL is in read-only mode.
//   * Semi-sync settings are consistent with a REPLICA tablet.
//
// If necessary, it waits for all in-flight writes to complete or time out.
//
// It should be safe to call this on a PRIMARY tablet that was already demoted,
// or on a tablet that already transitioned to REPLICA.
//
// If a step fails in the middle, it will try to undo any changes it made.
func (tm *TabletManager) DemotePrimary(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	log.Infof("DemotePrimary")
	// The public version always reverts on partial failure.
	return tm.demotePrimary(ctx, true /* revertPartialFailure */)
}

// DemoteMaster is the old version of DemotePrimary. Deprecated.
func (tm *TabletManager) DemoteMaster(ctx context.Context) (*replicationdatapb.PrimaryStatus, error) {
	return tm.DemotePrimary(ctx)
}

// demotePrimary implements DemotePrimary with an additional, private option.
//
// If revertPartialFailure is true, and a step fails in the middle, it will try
// to undo any changes it made.
func (tm *TabletManager) demotePrimary(ctx context.Context, revertPartialFailure bool) (primaryStatus *replicationdatapb.PrimaryStatus, finalErr error) {
	if err := tm.lock(ctx); err != nil {
		return nil, err
	}
	defer tm.unlock()

	tablet := tm.Tablet()
	wasPrimary := tablet.Type == topodatapb.TabletType_PRIMARY
	wasServing := tm.QueryServiceControl.IsServing()
	wasReadOnly, err := tm.MysqlDaemon.IsReadOnly()
	if err != nil {
		return nil, err
	}

	// If we are a primary tablet and not yet read-only, stop accepting new
	// queries and wait for in-flight queries to complete. If we are not primary,
	// or if we are already read-only, there's no need to stop the queryservice
	// in order to ensure the guarantee we are being asked to provide, which is
	// that no writes are occurring.
	if wasPrimary && !wasReadOnly {
		// Tell Orchestrator we're stopped on purpose for demotion.
		// This is a best effort task, so run it in a goroutine.
		go func() {
			if tm.orc == nil {
				return
			}
			if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to DemotePrimary"); err != nil {
				log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			}
		}()

		// Note that this may block until the transaction timeout if clients
		// don't finish their transactions in time. Even if some transactions
		// have to be killed at the end of their timeout, this will be
		// considered successful. If we are already not serving, this will be
		// idempotent.
		log.Infof("DemotePrimary disabling query service")
		if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), false, "demotion in progress"); err != nil {
			return nil, vterrors.Wrap(err, "SetServingType(serving=false) failed")
		}
		defer func() {
			if finalErr != nil && revertPartialFailure && wasServing {
				if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), true, ""); err != nil {
					log.Warningf("SetServingType(serving=true) failed during revert: %v", err)
				}
			}
		}()
	}

	// Now that we know no writes are in-flight and no new writes can occur,
	// set MySQL to read-only mode. If we are already read-only because of a
	// previous demotion, or because we are not primary anyway, this should be
	// idempotent.
	if *setSuperReadOnly {
		// Setting super_read_only also sets read_only
		if err := tm.MysqlDaemon.SetSuperReadOnly(true); err != nil {
			return nil, err
		}
	} else {
		if err := tm.MysqlDaemon.SetReadOnly(true); err != nil {
			return nil, err
		}
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && !wasReadOnly {
			// setting read_only OFF will also set super_read_only OFF if it was set
			if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
				log.Warningf("SetReadOnly(false) failed during revert: %v", err)
			}
		}
	}()

	// If using semi-sync, we need to disable primary-side.
	if err := tm.fixSemiSync(topodatapb.TabletType_REPLICA); err != nil {
		return nil, err
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && wasPrimary {
			// enable primary-side semi-sync again
			if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY); err != nil {
				log.Warningf("fixSemiSync(PRIMARY) failed during revert: %v", err)
			}
		}
	}()

	// Return the current replication position.
	status, err := tm.MysqlDaemon.PrimaryStatus(ctx)
	if err != nil {
		return nil, err
	}
	return mysql.PrimaryStatusToProto(status), nil
}

// UndoDemoteMaster is the old version of UndoDemotePrimary. Deprecated.
func (tm *TabletManager) UndoDemoteMaster(ctx context.Context) error {
	return tm.UndoDemotePrimary(ctx)
}

// UndoDemotePrimary reverts a previous call to DemotePrimary
// it sets read-only to false, fixes semi-sync
// and returns its primary position.
func (tm *TabletManager) UndoDemotePrimary(ctx context.Context) error {
	log.Infof("UndoDemotePrimary")
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// If using semi-sync, we need to enable source-side.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY); err != nil {
		return err
	}

	// Now, set the server read-only false.
	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return err
	}

	// Update serving graph
	tablet := tm.Tablet()
	log.Infof("UndoDemotePrimary re-enabling query service")
	if err := tm.QueryServiceControl.SetServingType(tablet.Type, logutil.ProtoToTime(tablet.PrimaryTermStartTime), true, ""); err != nil {
		return vterrors.Wrap(err, "SetServingType(serving=true) failed")
	}
	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()
	return nil
}

// ReplicaWasPromoted promotes a replica to primary, no questions asked.
func (tm *TabletManager) ReplicaWasPromoted(ctx context.Context) error {
	log.Infof("ReplicaWasPromoted")
	return tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY)
}

// SetReplicationSource sets replication primary, and waits for the
// reparent_journal table entry up to context timeout
func (tm *TabletManager) SetReplicationSource(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	log.Infof("SetReplicationSource: parent: %v  position: %v force: %v", parentAlias, waitPosition, forceStartReplication)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	return tm.setReplicationSourceLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication)
}

// SetMaster is the old version of SetReplicationSource. Deprecated.
func (tm *TabletManager) SetMaster(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) error {
	return tm.SetReplicationSource(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication)
}

func (tm *TabletManager) setReplicationSourceRepairReplication(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) (err error) {
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	ctx, unlock, lockErr := tm.TopoServer.LockShard(ctx, parent.Tablet.GetKeyspace(), parent.Tablet.GetShard(), fmt.Sprintf("repairReplication to %v as parent)", topoproto.TabletAliasString(parentAlias)))
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	return tm.setReplicationSourceLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartReplication)
}

func (tm *TabletManager) setReplicationSourceLocked(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool) (err error) {
	// End orchestrator maintenance at the end of fixing replication.
	// This is a best effort operation, so it should happen in a goroutine
	defer func() {
		go func() {
			if tm.orc == nil {
				return
			}
			if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
				log.Warningf("Orchestrator EndMaintenance failed: %v", err)
			}
		}()
	}()

	// Change our type to REPLICA if we used to be PRIMARY.
	// Being sent SetReplicationSource means another PRIMARY has been successfully promoted,
	// so we convert to REPLICA first, since we want to do it even if other
	// steps fail below.
	// Note it is important to check for PRIMARY here so that we don't
	// unintentionally change the type of RDONLY tablets
	tablet := tm.Tablet()
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone); err != nil {
			return err
		}
	}

	// See if we were replicating at all, and should be replicating.
	wasReplicating := false
	shouldbeReplicating := false
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err == mysql.ErrNotReplica {
		// This is a special error that means we actually succeeded in reading
		// the status, but the status is empty because replication is not
		// configured. We assume this means we used to be a primary, so we always
		// try to start replicating once we are told who the new primary is.
		shouldbeReplicating = true
		// Since we continue in the case of this error, make sure 'status' is
		// in a known, empty state.
		status = mysql.ReplicationStatus{}
	} else if err != nil {
		// Abort on any other non-nil error.
		return err
	}
	if status.IOThreadRunning || status.SQLThreadRunning {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartReplication {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to primary.
	// If we are currently PRIMARY, assume we are about to become REPLICA.
	tabletType := tm.Tablet().Type
	if tabletType == topodatapb.TabletType_PRIMARY {
		tabletType = topodatapb.TabletType_REPLICA
	}
	if err := tm.fixSemiSync(tabletType); err != nil {
		return err
	}
	// Update the primary/source address only if needed.
	// We don't want to interrupt replication for no reason.
	if parentAlias == nil {
		// if there is no primary in the shard, return an error so that we can retry
		return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "Shard primaryAlias is nil")
	}
	parent, err := tm.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}
	host := parent.Tablet.MysqlHostname
	port := int(parent.Tablet.MysqlPort)
	if status.SourceHost != host || status.SourcePort != port {
		// This handles both changing the address and starting replication.
		if err := tm.MysqlDaemon.SetReplicationSource(ctx, host, port, wasReplicating, shouldbeReplicating); err != nil {
			if err := tm.handleRelayLogError(err); err != nil {
				return err
			}
		}
	} else if shouldbeReplicating {
		// The address is correct. Just start replication if needed.
		if !status.ReplicationRunning() {
			if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
				if err := tm.handleRelayLogError(err); err != nil {
					return err
				}
			}
		}
	}

	// If needed, wait until we replicate to the specified point, or our context
	// times out. Callers can specify the point to wait for as either a
	// GTID-based replication position or a Vitess reparent journal entry,
	// or both.
	if shouldbeReplicating {
		if waitPosition != "" {
			pos, err := mysql.DecodePosition(waitPosition)
			if err != nil {
				return err
			}
			if err := tm.MysqlDaemon.WaitSourcePos(ctx, pos); err != nil {
				return err
			}
		}
		if timeCreatedNS != 0 {
			if err := tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
				return err
			}
		}
		// Clear replication sentinel flag for this replica
		tm.replManager.setReplicationStopped(false)
	}

	return nil
}

// ReplicaWasRestarted updates the parent record for a tablet.
func (tm *TabletManager) ReplicaWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	log.Infof("ReplicaWasRestarted: parent: %v", parent)
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	// Only change type of former PRIMARY tablets.
	// Don't change type of RDONLY
	tablet := tm.Tablet()
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		return nil
	}
	return tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone)
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status.
func (tm *TabletManager) StopReplicationAndGetStatus(ctx context.Context, stopReplicationMode replicationdatapb.StopReplicationMode) (StopReplicationAndGetStatusResponse, error) {
	log.Infof("StopReplicationAndGetStatus: mode: %v", stopReplicationMode)
	if err := tm.lock(ctx); err != nil {
		return StopReplicationAndGetStatusResponse{}, err
	}
	defer tm.unlock()

	// Get the status before we stop replication.
	// Doing this first allows us to return the status in the case that stopping replication
	// returns an error, so a user can optionally inspect the status before a stop was called.
	rs, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return StopReplicationAndGetStatusResponse{}, vterrors.Wrap(err, "before status failed")
	}
	before := mysql.ReplicationStatusToProto(rs)

	if stopReplicationMode == replicationdatapb.StopReplicationMode_IOTHREADONLY {
		if !rs.IOThreadRunning {
			return StopReplicationAndGetStatusResponse{
				HybridStatus: before,
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
					After:  before,
				},
			}, nil
		}
		if err := tm.stopIOThreadLocked(ctx); err != nil {
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
				},
			}, vterrors.Wrap(err, "stop io thread failed")
		}
	} else {
		if !rs.IOThreadRunning && !rs.SQLThreadRunning {
			// no replication is running, just return what we got
			return StopReplicationAndGetStatusResponse{
				HybridStatus: before,
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
					After:  before,
				},
			}, nil
		}
		if err := tm.stopReplicationLocked(ctx); err != nil {
			return StopReplicationAndGetStatusResponse{
				Status: &replicationdatapb.StopReplicationStatus{
					Before: before,
				},
			}, vterrors.Wrap(err, "stop replication failed")
		}
	}

	// Get the status after we stop replication so we have up to date position and relay log positions.
	rsAfter, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		return StopReplicationAndGetStatusResponse{
			Status: &replicationdatapb.StopReplicationStatus{
				Before: before,
			},
		}, vterrors.Wrap(err, "acquiring replication status failed")
	}
	after := mysql.ReplicationStatusToProto(rsAfter)

	rs.Position = rsAfter.Position
	rs.RelayLogPosition = rsAfter.RelayLogPosition
	rs.FilePosition = rsAfter.FilePosition
	rs.FileRelayLogPosition = rsAfter.FileRelayLogPosition

	return StopReplicationAndGetStatusResponse{
		HybridStatus: mysql.ReplicationStatusToProto(rs),
		Status: &replicationdatapb.StopReplicationStatus{
			Before: before,
			After:  after,
		},
	}, nil
}

// StopReplicationAndGetStatusResponse holds the original hybrid Status struct, as well as a new Status field, which
// hold the result of show replica status called before stopping replication, and after stopping replication.
type StopReplicationAndGetStatusResponse struct {
	// HybridStatus is deprecated. It currently represents a hybrid struct where all data represents the before state,
	// except for all position related data which comes from the after state. Please use status instead, which holds
	// discrete replication status calls before and after stopping the replica, or stopping the replica's io_thread.
	HybridStatus *replicationdatapb.Status

	// Status represents the replication status call right before, and right after telling the replica to stop.
	Status *replicationdatapb.StopReplicationStatus
}

// PromoteReplica makes the current tablet the primary
func (tm *TabletManager) PromoteReplica(ctx context.Context) (string, error) {
	log.Infof("PromoteReplica")
	if err := tm.lock(ctx); err != nil {
		return "", err
	}
	defer tm.unlock()

	pos, err := tm.MysqlDaemon.Promote(tm.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := tm.fixSemiSync(topodatapb.TabletType_PRIMARY); err != nil {
		return "", err
	}

	if err := tm.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	// Clear replication sentinel flag for this primary,
	// or we might block replication the next time we demote it
	tm.replManager.setReplicationStopped(false)

	return mysql.EncodePosition(pos), nil
}

func isPrimaryEligible(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		return true
	}

	return false
}

func (tm *TabletManager) fixSemiSync(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	// Only enable if we're eligible for becoming primary (REPLICA type).
	// Ineligible tablets (RDONLY) shouldn't ACK because we'll never promote them.
	if !isPrimaryEligible(tabletType) {
		return tm.MysqlDaemon.SetSemiSyncEnabled(false, false)
	}

	// Always enable replica-side since it doesn't hurt to keep it on for a primary.
	// The primary-side needs to be off for a replica, or else it will get stuck.
	return tm.MysqlDaemon.SetSemiSyncEnabled(tabletType == topodatapb.TabletType_PRIMARY, true)
}

func (tm *TabletManager) fixSemiSyncAndReplication(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	if tabletType == topodatapb.TabletType_PRIMARY {
		// Primary is special. It is always handled at the
		// right time by the reparent operations, it doesn't
		// need to be fixed.
		return nil
	}

	if err := tm.fixSemiSync(tabletType); err != nil {
		return vterrors.Wrapf(err, "failed to fixSemiSync(%v)", tabletType)
	}

	// If replication is running, but the status is wrong,
	// we should restart replication. First, let's make sure
	// replication is running.
	status, err := tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		// Replication is not configured, nothing to do.
		return nil
	}
	if !status.IOThreadRunning {
		// IO thread is not running, nothing to do.
		return nil
	}

	shouldAck := isPrimaryEligible(tabletType)
	acking, err := tm.MysqlDaemon.SemiSyncReplicationStatus()
	if err != nil {
		return vterrors.Wrap(err, "failed to get SemiSyncReplicationStatus")
	}
	if shouldAck == acking {
		return nil
	}

	// We need to restart replication
	log.Infof("Restarting replication for semi-sync flag change to take effect from %v to %v", acking, shouldAck)
	if err := tm.MysqlDaemon.StopReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StopReplication")
	}
	if err := tm.MysqlDaemon.StartReplication(tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StartReplication")
	}
	return nil
}

func (tm *TabletManager) handleRelayLogError(err error) error {
	// attempt to fix this error:
	// Slave failed to initialize relay log info structure from the repository (errno 1872) (sqlstate HY000) during query: START SLAVE
	// see https://bugs.mysql.com/bug.php?id=83713 or https://github.com/vitessio/vitess/issues/5067
	if strings.Contains(err.Error(), "Slave failed to initialize relay log info structure from the repository") {
		// Stop, reset and start replication again to resolve this error
		if err := tm.MysqlDaemon.RestartReplication(tm.hookExtraEnv()); err != nil {
			return err
		}
		return nil
	}
	return err
}

// repairReplication tries to connect this server to whoever is
// the current primary of the shard, and start replicating.
func (tm *TabletManager) repairReplication(ctx context.Context) error {
	tablet := tm.Tablet()

	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if !si.HasPrimary() {
		return fmt.Errorf("no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	if topoproto.TabletAliasEqual(si.PrimaryAlias, tablet.Alias) {
		// The shard record says we are primary, but we disagree; we wouldn't
		// reach this point unless we were told to check replication.
		// Hopefully someone is working on fixing that, but in any case,
		// we should not try to reparent to ourselves.
		return fmt.Errorf("shard %v/%v record claims tablet %v is primary, but its type is %v", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(tablet.Alias), tablet.Type)
	}

	// If Orchestrator is configured and if Orchestrator is actively reparenting, we should not repairReplication
	if tm.orc != nil {
		re, err := tm.orc.InActiveShardRecovery(tablet)
		if err != nil {
			return err
		}
		if re {
			return fmt.Errorf("orchestrator actively reparenting shard %v, skipping repairReplication", si)
		}

		// Before repairing replication, tell Orchestrator to enter maintenance mode for this tablet and to
		// lock any other actions on this tablet by Orchestrator.
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to StopReplication"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			return vterrors.Wrap(err, "orchestrator BeginMaintenance failed, skipping repairReplication")
		}
	}

	return tm.setReplicationSourceRepairReplication(ctx, si.PrimaryAlias, 0, "", true)
}
