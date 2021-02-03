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

package database

import (
	"github.com/XiaoMi/Gaea/core"
	"time"
)

type TxProperties struct {
	userName   string
	remoteHost string
	StartTime  time.Time
	EndTime    time.Time
	Queries    []string
	Autocommit bool
	Conclusion string
}

//NewTxProps creates a new TxProperties struct
func (tp *TxPool) NewTxPropsFromCaller(caller Caller, autocommit bool) *TxProperties {
	return &TxProperties{
		StartTime:  time.Now(),
		userName:   caller.User(),
		remoteHost: caller.From(),
		Autocommit: autocommit,
		//Stats:      tp.txStats,
	}
}

//NewTxProps creates a new TxProperties struct
func (tp *TxPool) NewTxProps(user, host string, autocommit bool) *TxProperties {
	return &TxProperties{
		StartTime:  time.Now(),
		userName:   user,
		remoteHost: host,
		Autocommit: autocommit,
		//Stats:      tp.txStats,
	}
}

func (p *TxProperties) User() string {
	return p.userName
}

func (p *TxProperties) From() string {
	return p.remoteHost
}

// RecordQuery records the query against this transaction.
func (p *TxProperties) RecordQuery(query string) {
	if p == nil {
		return
	}
	p.Queries = append(p.Queries, query)
}

// InTransaction returns true as soon as this struct is not nil
func (p *TxProperties) InTransaction() bool { return p != nil }

// String returns a printable version of the transaction
func (p *TxProperties) String() string {
	if p == nil {
		return ""
	}

	sb := core.NewStringBuilder()
	if p.remoteHost != "" {
		sb.Write("host:", p.remoteHost, ", ")
	}

	if p.userName != "" {
		sb.Write("user:", p.userName, ", ")
	}

	s := p.StartTime.Format("2006-01-02 15:04:05")
	sb.Write("start:", s, ", ")

	if !p.EndTime.IsZero() {
		sb.Write("end:", p.EndTime.Format("2006-01-02 15:04:05"), ", ")
		sb.Write("live:", p.EndTime.Sub(p.StartTime).Seconds(), "s, ")
	}

	if p.Conclusion != "" {
		sb.Write(p.Conclusion, ", ")
	}

	if len(p.Queries) > 0 {
		sb.WriteLine()
		sb.WriteLineForEach(p.Queries)
	}

	return sb.String()
}
