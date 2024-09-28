/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

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

package vtgate

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TxConn is used for executing transactional requests.
type TxConn struct {
	tabletGateway *TabletGateway
	mode          vtgatepb.TransactionMode
}

// NewTxConn builds a new TxConn.
func NewTxConn(gw *TabletGateway, txMode vtgatepb.TransactionMode) *TxConn {
	return &TxConn{
		tabletGateway: gw,
		mode:          txMode,
	}
}

var txAccessModeToEOTxAccessMode = map[sqlparser.TxAccessMode]querypb.ExecuteOptions_TransactionAccessMode{
	sqlparser.WithConsistentSnapshot: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT,
	sqlparser.ReadWrite:              querypb.ExecuteOptions_READ_WRITE,
	sqlparser.ReadOnly:               querypb.ExecuteOptions_READ_ONLY,
}

// Begin begins a new transaction. If one is already in progress, it commits it
// and starts a new one.
func (txc *TxConn) Begin(ctx context.Context, session *SafeSession, txAccessModes []sqlparser.TxAccessMode) error {
	if session.InTransaction() {
		if err := txc.Commit(ctx, session); err != nil {
			return err
		}
	}
	if len(txAccessModes) > 0 {
		options := session.GetOrCreateOptions()
		for _, txAccessMode := range txAccessModes {
			accessMode, ok := txAccessModeToEOTxAccessMode[txAccessMode]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] invalid transaction characteristic: %s", txAccessMode.ToString())
			}
			options.TransactionAccessMode = append(options.TransactionAccessMode, accessMode)
		}
	}
	session.Session.InTransaction = true
	return nil
}

// Commit commits the current transaction. The type of commit can be
// best effort or 2pc depending on the session setting.
func (txc *TxConn) Commit(ctx context.Context, session *SafeSession) error {
	defer session.ResetTx()
	if !session.InTransaction() {
		return nil
	}

	twopc := false
	switch session.TransactionMode {
	case vtgatepb.TransactionMode_TWOPC:
		twopc = true
	case vtgatepb.TransactionMode_UNSPECIFIED:
		twopc = txc.mode == vtgatepb.TransactionMode_TWOPC
	}

	if twopc {
		return txc.commit2PC(ctx, session)
	}
	return txc.commitNormal(ctx, session)
}

func (txc *TxConn) queryService(alias *topodatapb.TabletAlias) (queryservice.QueryService, error) {
	if alias == nil {
		return txc.tabletGateway, nil
	}
	return txc.tabletGateway.QueryServiceByAlias(alias, nil)
}

func (txc *TxConn) commitShard(ctx context.Context, session *SafeSession, s *vtgatepb.Session_ShardSession, logging *executeLogger) error {
	if s.TransactionId == 0 {
		return nil
	}
	var qs queryservice.QueryService
	var err error
	qs, err = txc.queryService(s.TabletAlias)
	if err != nil {
		return err
	}
	reservedID, sessionStateChange, err := qs.Commit(ctx, s.Target, s.TransactionId)
	if err != nil {
		return err
	}
	s.TransactionId = 0
	s.ReservedId = reservedID
	if sessionStateChange != "" {
		session.SetReadAfterWriteGTID(sessionStateChange)
		txc.tabletGateway.AddGtid(sessionStateChange)

		// table level RAW
		for tableName, _ := range txc.tabletGateway.tableNamesMap {
			// session
			session.UpdateReadAfterReadGTIDMap(tableName,
				txc.tabletGateway.LastSeenGtidString())
			// instance
			txc.tabletGateway.latestGTIDForTable.UpdateGTID(tableName,
				txc.tabletGateway.LastSeenGtidString())
		}

	}
	txc.tabletGateway.tableNamesMap = make(map[string]bool)
	logging.log(nil, s.Target, nil, "commit", false, nil)
	return nil
}

func (txc *TxConn) commitNormal(ctx context.Context, session *SafeSession) error {
	// wrap txc.commitShard in a closure so that it can be used in runSessions
	action := func(session *SafeSession) func(context.Context, *vtgatepb.Session_ShardSession, *executeLogger) error {
		return func(ctx context.Context, shardSession *vtgatepb.Session_ShardSession, logging *executeLogger) error {
			return txc.commitShard(ctx, session, shardSession, logging)
		}
	}(session)
	if err := txc.runSessions(ctx, session.PreSessions, session.logging, action); err != nil {
		_ = txc.Release(ctx, session)
		return err
	}

	// Retain backward compatibility on commit order for the normal session.
	for _, shardSession := range session.ShardSessions {
		if err := txc.commitShard(ctx, session, shardSession, session.logging); err != nil {
			_ = txc.Release(ctx, session)
			return err
		}
	}

	if err := txc.runSessions(ctx, session.PostSessions, session.logging, action); err != nil {
		// If last commit fails, there will be nothing to rollback.
		session.RecordWarning(&querypb.QueryWarning{Message: fmt.Sprintf("post-operation transaction had an error: %v", err)})
		// With reserved connection we should release them.
		if session.InReservedConn() {
			_ = txc.Release(ctx, session)
		}
	}
	return nil
}

// commit2PC will not used the pinned tablets - to make sure we use the current source, we need to use the gateway's queryservice
func (txc *TxConn) commit2PC(ctx context.Context, session *SafeSession) error {
	if len(session.PreSessions) != 0 || len(session.PostSessions) != 0 {
		_ = txc.Rollback(ctx, session)
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "pre or post actions not allowed for 2PC commits")
	}

	// If the number of participants is one or less, then it's a normal commit.
	if len(session.ShardSessions) <= 1 {
		return txc.commitNormal(ctx, session)
	}

	participants := make([]*querypb.Target, 0, len(session.ShardSessions)-1)
	for _, s := range session.ShardSessions[1:] {
		participants = append(participants, s.Target)
	}
	mmShard := session.ShardSessions[0]
	dtid := dtids.New(mmShard)
	err := txc.tabletGateway.CreateTransaction(ctx, mmShard.Target, dtid, participants)
	if err != nil {
		// Normal rollback is safe because nothing was prepared yet.
		_ = txc.Rollback(ctx, session)
		return err
	}

	err = txc.runSessions(ctx, session.ShardSessions[1:], session.logging, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *executeLogger) error {
		return txc.tabletGateway.Prepare(ctx, s.Target, s.TransactionId, dtid)
	})
	if err != nil {
		// TODO(sougou): Perform a more fine-grained cleanup
		// including unprepared transactions.
		if resumeErr := txc.Resolve(ctx, dtid); resumeErr != nil {
			log.Warningf("Rollback failed after Prepare failure: %v", resumeErr)
		}
		// Return the original error even if the previous operation fails.
		return err
	}

	err = txc.tabletGateway.StartCommit(ctx, mmShard.Target, mmShard.TransactionId, dtid)
	if err != nil {
		return err
	}

	err = txc.runSessions(ctx, session.ShardSessions[1:], session.logging, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *executeLogger) error {
		return txc.tabletGateway.CommitPrepared(ctx, s.Target, dtid)
	})
	if err != nil {
		return err
	}

	return txc.tabletGateway.ConcludeTransaction(ctx, mmShard.Target, dtid)
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (txc *TxConn) Rollback(ctx context.Context, session *SafeSession) error {
	if !session.InTransaction() {
		return nil
	}
	defer session.ResetTx()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)

	err := txc.runSessions(ctx, allsessions, session.logging, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *executeLogger) error {
		if s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(s.TabletAlias)
		if err != nil {
			return err
		}
		reservedID, err := qs.Rollback(ctx, s.Target, s.TransactionId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = reservedID
		logging.log(nil, s.Target, nil, "rollback", false, nil)
		return nil
	})
	if err != nil {
		session.RecordWarning(&querypb.QueryWarning{Message: fmt.Sprintf("rollback encountered an error and connection to all shard for this session is released: %v", err)})
		if session.InReservedConn() {
			_ = txc.Release(ctx, session)
		}
	}
	return err
}

// Release releases the reserved connection and/or rollbacks the transaction
func (txc *TxConn) Release(ctx context.Context, session *SafeSession) error {
	if !session.InTransaction() && !session.InReservedConn() {
		return nil
	}
	defer session.Reset()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)

	return txc.runSessions(ctx, allsessions, session.logging, func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *executeLogger) error {
		if s.ReservedId == 0 && s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(s.TabletAlias)
		if err != nil {
			return err
		}
		err = qs.Release(ctx, s.Target, s.TransactionId, s.ReservedId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = 0
		return nil
	})
}

// ReleaseLock releases the reserved connection used for locking.
func (txc *TxConn) ReleaseLock(ctx context.Context, session *SafeSession) error {
	if !session.InLockSession() {
		return nil
	}
	defer session.ResetLock()

	session.ClearAdvisoryLock()
	ls := session.LockSession
	if ls.ReservedId == 0 {
		return nil
	}
	qs, err := txc.queryService(ls.TabletAlias)
	if err != nil {
		return err
	}
	return qs.Release(ctx, ls.Target, 0, ls.ReservedId)
}

// ReleaseAll releases all the shard sessions and lock session.
func (txc *TxConn) ReleaseAll(ctx context.Context, session *SafeSession) error {
	if !session.InTransaction() && !session.InReservedConn() && !session.InLockSession() {
		return nil
	}
	defer session.ResetAll()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)
	if session.LockSession != nil {
		allsessions = append(allsessions, session.LockSession)
	}

	return txc.runSessions(ctx, allsessions, session.logging, func(ctx context.Context, s *vtgatepb.Session_ShardSession, loggging *executeLogger) error {
		if s.ReservedId == 0 && s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(s.TabletAlias)
		if err != nil {
			return err
		}
		err = qs.Release(ctx, s.Target, s.TransactionId, s.ReservedId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = 0
		return nil
	})
}

// Resolve resolves the specified 2PC transaction.
func (txc *TxConn) Resolve(ctx context.Context, dtid string) error {
	mmShard, err := dtids.ShardSession(dtid)
	if err != nil {
		return err
	}

	transaction, err := txc.tabletGateway.ReadTransaction(ctx, mmShard.Target, dtid)
	if err != nil {
		return err
	}
	if transaction == nil || transaction.Dtid == "" {
		// It was already resolved.
		return nil
	}
	switch transaction.State {
	case querypb.TransactionState_PREPARE:
		// If state is PREPARE, make a decision to rollback and
		// fallthrough to the rollback workflow.
		qs, err := txc.queryService(mmShard.TabletAlias)
		if err != nil {
			return err
		}
		if err := qs.SetRollback(ctx, mmShard.Target, transaction.Dtid, mmShard.TransactionId); err != nil {
			return err
		}
		fallthrough
	case querypb.TransactionState_ROLLBACK:
		if err := txc.resumeRollback(ctx, mmShard.Target, transaction); err != nil {
			return err
		}
	case querypb.TransactionState_COMMIT:
		if err := txc.resumeCommit(ctx, mmShard.Target, transaction); err != nil {
			return err
		}
	default:
		// Should never happen.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid state: %v", transaction.State)
	}
	return nil
}

func (txc *TxConn) resumeRollback(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.tabletGateway.RollbackPrepared(ctx, t, transaction.Dtid, 0)
	})
	if err != nil {
		return err
	}
	return txc.tabletGateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

func (txc *TxConn) resumeCommit(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.tabletGateway.CommitPrepared(ctx, t, transaction.Dtid)
	})
	if err != nil {
		return err
	}
	return txc.tabletGateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

// runSessions executes the action for all shardSessions in parallel and returns a consolidated error.
func (txc *TxConn) runSessions(ctx context.Context, shardSessions []*vtgatepb.Session_ShardSession, logging *executeLogger, action func(context.Context, *vtgatepb.Session_ShardSession, *executeLogger) error) error {
	// Fastpath.
	if len(shardSessions) == 1 {
		return action(ctx, shardSessions[0], logging)
	}

	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, s := range shardSessions {
		wg.Add(1)
		go func(s *vtgatepb.Session_ShardSession) {
			defer wg.Done()
			if err := action(ctx, s, logging); err != nil {
				allErrors.RecordError(err)
			}
		}(s)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

// runTargets executes the action for all targets in parallel and returns a consolildated error.
// Flow is identical to runSessions.
func (txc *TxConn) runTargets(targets []*querypb.Target, action func(*querypb.Target) error) error {
	if len(targets) == 1 {
		return action(targets[0])
	}
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t *querypb.Target) {
			defer wg.Done()
			if err := action(t); err != nil {
				allErrors.RecordError(err)
			}
		}(t)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}
