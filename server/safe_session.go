/*
 * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  File author: Anders Xiao
 */

package server

import (
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"sync"
	"time"
)

// SafeSession is a mutex-protected version of the Session.
// It is thread-safe if each thread only accesses one shard.
// (the use pattern is 'Find', if not found, then 'AppendOrUpdate',
// for a single shard)
type SafeSession struct {
	mu              sync.Mutex
	mustRollback    bool
	autocommitState autocommitState
	commitOrder     CommitOrder

	// this is a signal that found_rows has already been handles by the primitives,
	// and doesn't have to be updated by the executor
	foundRowsHandled bool
	*Session
}

// autocommitState keeps track of whether a single round-trip
// commit to vttablet is possible. It starts as autocommitable
// if we started a transaction because of the autocommit flag
// being set. Otherwise, it starts as notAutocommitable.
// If execute is recursively called using the same session,
// like from a vindex, we will already be in a transaction,
// and this should cause the state to become notAutocommitable.
//
// SafeSession lets you request a commit token, which will
// be issued if the state is autocommitable,
// implying that no intermediate transactions were started.
// If so, the state transitions to autocommited, which is terminal.
// If the token is successfully issued, the caller has to perform
// the commit. If a token cannot be issued, then a traditional
// commit has to be performed at the outermost level where
// the autocommitable transition happened.
type autocommitState int

const (
	notAutocommittable autocommitState = iota
	autocommittable
	autocommitted
)

// NewSafeSession returns a new SafeSession based on the Session
func NewSafeSession(sessn *Session) *SafeSession {
	if sessn == nil {
		sessn = &Session{}
	}
	return &SafeSession{Session: sessn}
}

// NewAutocommitSession returns a SafeSession based on the original
// session, but with autocommit enabled.
func NewAutocommitSession(sessn *Session) (*SafeSession, error) {
	newSession := &Session{}
	if err := util.JsonClone(newSession, sessn); err != nil {
		return nil, err
	}
	newSession.InTransaction = false
	newSession.ShardSessions = nil
	newSession.PreSessions = nil
	newSession.PostSessions = nil
	newSession.Autocommit = true
	newSession.Warnings = nil
	return NewSafeSession(newSession), nil
}

// ResetTx clears the session
func (session *SafeSession) ResetTx() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = CommitOrderNormal
	session.Savepoints = nil
	if !session.Session.InReservedConn {
		session.ShardSessions = nil
		session.PreSessions = nil
		session.PostSessions = nil
	}
}

// Reset clears the session
func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = CommitOrderNormal
	session.Savepoints = nil
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
}

// SetAutocommittable sets the state to autocommitable if true.
// Otherwise, it's notAutocommitable.
func (session *SafeSession) SetAutocommittable(flag bool) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.autocommitState == autocommitted {
		// Unreachable.
		return
	}

	if flag {
		session.autocommitState = autocommittable
	} else {
		session.autocommitState = notAutocommittable
	}
}

// AutocommitApproval returns true if we can perform a single round-trip
// autocommit. If so, the caller is responsible for committing their
// transaction.
func (session *SafeSession) AutocommitApproval() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.autocommitState == autocommitted {
		// Unreachable.
		return false
	}

	if session.autocommitState == autocommittable {
		session.autocommitState = autocommitted
		return true
	}
	return false
}

// SetCommitOrder sets the commit order.
func (session *SafeSession) SetCommitOrder(co CommitOrder) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.commitOrder = co
}

// InTransaction returns true if we are in a transaction
func (session *SafeSession) InTransaction() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

// Find returns the transactionId and tabletAlias, if any, for a session
func (session *SafeSession) Find(schema, shard string, tabletType database.TabletType) (transactionID int64, reservedID int64, target *database.Target) {
	session.mu.Lock()
	defer session.mu.Unlock()
	sessions := session.ShardSessions
	switch session.commitOrder {
	case CommitOrderPre:
		sessions = session.PreSessions
	case CommitOrderPost:
		sessions = session.PostSessions
	}
	for _, shardSession := range sessions {
		if schema == shardSession.Target.Schema && tabletType == shardSession.Target.TabletType && shard == shardSession.Target.DataSource {
			return shardSession.TransactionId, shardSession.ReservedId, shardSession.Target
		}
	}
	return 0, 0, nil
}

func addOrUpdate(shardSession *database.DbSession, sessions []*database.DbSession) []*database.DbSession {
	appendSession := true
	for i, sess := range sessions {
		targetedAtSameTablet := sess.Target.Schema == shardSession.Target.Schema &&
			sess.Target.TabletType == shardSession.Target.TabletType &&
			sess.Target.DataSource == shardSession.Target.DataSource
		if targetedAtSameTablet {
			// replace the old info with the new one
			sessions[i] = shardSession
			appendSession = false
			break
		}
	}
	if appendSession {
		sessions = append(sessions, shardSession)
	}

	return sessions
}

// AppendOrUpdate adds a new DbSession, or updates an existing one if one already exists for the given shard session
func (session *SafeSession) AppendOrUpdate(shardSession *database.DbSession, txMode TransactionMode) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// additional check of transaction id is required
	// as now in autocommit mode there can be session due to reserved connection
	// that needs to be stored as shard session.
	if session.autocommitState == autocommitted && shardSession.TransactionId != 0 {
		// Should be unreachable
		return errors.New("BUG: SafeSession.AppendOrUpdate: unexpected autocommit state")
	}
	if !(session.Session.InTransaction || session.Session.InReservedConn) {
		// Should be unreachable
		return errors.New("BUG: SafeSession.AppendOrUpdate: not in transaction and not in reserved connection")
	}
	session.autocommitState = notAutocommittable

	// Always append, in order for rollback to succeed.
	switch session.commitOrder {
	case CommitOrderNormal:
		newSessions := addOrUpdate(shardSession, session.ShardSessions)
		session.ShardSessions = newSessions
		// isSingle is enforced only for normmal commit order operations.
		if session.isSingleDB(txMode) && len(session.ShardSessions) > 1 {
			session.mustRollback = true
			return fmt.Errorf("multi-db transaction attempted: %v", session.ShardSessions)
		}
	case CommitOrderPre:
		newSessions := addOrUpdate(shardSession, session.PreSessions)
		session.PreSessions = newSessions
	case CommitOrderPost:
		newSessions := addOrUpdate(shardSession, session.PostSessions)
		session.PostSessions = newSessions
	default:
		// Should be unreachable
		return fmt.Errorf("BUG: SafeSession.AppendOrUpdate: unexpected commitOrder")
	}

	return nil
}

func (session *SafeSession) isSingleDB(txMode TransactionMode) bool {
	return session.TransactionMode == TransactionModeSingle ||
		(session.TransactionMode == TransactionModeUnspecified && txMode == TransactionModeSingle)
}

// SetRollback sets the flag indicating that the transaction must be rolled back.
// The call is a no-op if the session is not in a transaction.
func (session *SafeSession) SetRollback() {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.Session.InTransaction {
		session.mustRollback = true
	}
}

// MustRollback returns true if the transaction must be rolled back.
func (session *SafeSession) MustRollback() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.mustRollback
}

// RecordWarning stores the given warning in the session
func (session *SafeSession) RecordWarning(warning string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.Warnings = append(session.Session.Warnings, warning)
}

// ClearWarnings removes all the warnings from the session
func (session *SafeSession) ClearWarnings() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.Warnings = nil
}

// SetUserDefinedVariable sets the user defined variable in the session.
func (session *SafeSession) SetUserDefinedVariable(key string, value *types.BindVariable) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.UserDefinedVariables == nil {
		session.UserDefinedVariables = make(map[string]*types.BindVariable)
	}
	session.UserDefinedVariables[key] = value
}

// SetTargetString sets the target string in the session.
func (session *SafeSession) SetTargetString(target string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.TargetString = target
}

//SetSystemVariable sets the system variable in th session.
func (session *SafeSession) SetSystemVariable(name string, expr string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.SystemVariables == nil {
		session.SystemVariables = make(map[string]string)
	}
	session.SystemVariables[name] = expr
}

// SetOptions sets the options
func (session *SafeSession) SetOptions(options *types.ExecuteOptions) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Options = options
}

// StoreSavepoint stores the savepoint and release savepoint queries in the session
func (session *SafeSession) StoreSavepoint(sql string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Savepoints = append(session.Savepoints, sql)
}

// InReservedConn returns true if the session needs to execute on a dedicated connection
func (session *SafeSession) InReservedConn() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InReservedConn
}

// SetReservedConn set the InReservedConn setting.
func (session *SafeSession) SetReservedConn(reservedConn bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.InReservedConn = reservedConn
}

// SetPreQueries returns the prequeries that need to be run when reserving a connection
func (session *SafeSession) SetPreQueries() []string {
	session.mu.Lock()
	defer session.mu.Unlock()
	result := make([]string, len(session.SystemVariables))
	idx := 0
	for k, v := range session.SystemVariables {
		result[idx] = fmt.Sprintf("set @@%s = %s", k, v)
		idx++
	}
	return result
}

// SetLockSession sets the lock session.
func (session *SafeSession) SetLockSession(lockSession *database.DbSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LockSession = lockSession
	session.LastLockHeartbeat = time.Now().Unix()
}

// UpdateLockHeartbeat updates the LastLockHeartbeat time
func (session *SafeSession) UpdateLockHeartbeat() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LastLockHeartbeat = time.Now().Unix()
}

// TriggerLockHeartBeat returns if it time to trigger next lock heartbeat
func (session *SafeSession) TriggerLockHeartBeat() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	now := time.Now().Unix()
	return now-session.LastLockHeartbeat >= int64(lockHeartbeatTime.Seconds())
}

// InLockSession returns whether locking is used on this session.
func (session *SafeSession) InLockSession() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.LockSession != nil
}

// ResetLock resets the lock session
func (session *SafeSession) ResetLock() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LockSession = nil
}

// ResetAll resets the shard sessions and lock session.
func (session *SafeSession) ResetAll() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = CommitOrderNormal
	session.Savepoints = nil
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
	session.LockSession = nil
}

// ResetShard reset the shard session for the provided tablet alias.
func (session *SafeSession) ResetShard(target *database.Target) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// Always append, in order for rollback to succeed.
	switch session.commitOrder {
	case CommitOrderNormal:
		newSessions, err := removeShard(target, session.ShardSessions)
		if err != nil {
			return err
		}
		session.ShardSessions = newSessions
	case CommitOrderPre:
		newSessions, err := removeShard(target, session.PreSessions)
		if err != nil {
			return err
		}
		session.PreSessions = newSessions
	case CommitOrderPost:
		newSessions, err := removeShard(target, session.PostSessions)
		if err != nil {
			return err
		}
		session.PostSessions = newSessions
	default:
		// Should be unreachable
		return fmt.Errorf("BUG: SafeSession.ResetShard: unexpected commitOrder")
	}
	return nil
}

// SetDDLStrategy set the DDLStrategy setting.
func (session *SafeSession) SetDDLStrategy(strategy string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.DDLStrategy = strategy
}

// GetDDLStrategy returns the DDLStrategy value.
func (session *SafeSession) GetDDLStrategy() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.DDLStrategy
}

// GetSessionUUID returns the SessionUUID value.
func (session *SafeSession) GetSessionUUID() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.SessionUUID
}

// SetSessionEnableSystemSettings set the SessionEnableSystemSettings setting.
func (session *SafeSession) SetSessionEnableSystemSettings(allow bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.EnableSystemSettings = allow
}

// GetSessionEnableSystemSettings returns the SessionEnableSystemSettings value.
func (session *SafeSession) GetSessionEnableSystemSettings() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.EnableSystemSettings
}

func removeShard(tabletAlias *database.Target, sessions []*database.DbSession) ([]*database.DbSession, error) {
	idx := -1
	for i, session := range sessions {
		if session.Target.IsSame(tabletAlias) {
			if session.TransactionId != 0 {
				return nil, errors.New("BUG: SafeSession.ResetShard: in transaction")
			}
			idx = i
		}
	}
	if idx == -1 {
		return sessions, nil
	}
	return append(sessions[:idx], sessions[idx+1:]...), nil
}

// GetOrCreateOptions will return the current options struct, or create one and return it if no-one exists
func (session *SafeSession) GetOrCreateOptions() *types.ExecuteOptions {
	if session.Session.Options == nil {
		session.Session.Options = &types.ExecuteOptions{}
	}
	return session.Session.Options
}
