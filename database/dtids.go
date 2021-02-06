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

// Package dtids contains dtid convenience functions.
package database

import (
	"fmt"
	"strconv"
	"strings"
)

// New generates a dtid based on Session_ShardSession.
func Dtid(session *DbSession) string {
	return fmt.Sprintf("%s:%s:%d", session.Target.Schema, session.Target.DataSource, session.TransactionId)
}

// DbSession builds a Session_ShardSession from a dtid.
func NewDbSession(dtid string) (*DbSession, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 3 {
		return nil, fmt.Errorf("invalid parts in dtid: %s", dtid)
	}
	target := &Target{
		Schema:     splits[0],
		DataSource: splits[1],
		TabletType: TabletTypeMaster,
	}
	txid, err := strconv.ParseInt(splits[2], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction id in dtid: %s", dtid)
	}
	return &DbSession{
		Target:        target,
		TransactionId: txid,
	}, nil
}

// TransactionID extracts the original transaction ID from the dtid.
func TransactionID(dtid string) (int64, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 3 {
		return 0, fmt.Errorf("invalid parts in dtid: %s", dtid)
	}
	txid, err := strconv.ParseInt(splits[2], 10, 0)
	if err != nil {
		return 0, fmt.Errorf("invalid transaction id in dtid: %s", dtid)
	}
	return txid, nil
}
