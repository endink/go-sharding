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

import "context"

var FakeCoordConn = &fakeCoordConn{}

// These vars and types are used only for TestExecutorResolveTransaction
var dtidCh = make(chan string)

type FakeCoordinator struct {
}

func (f FakeCoordinator) Connect(ctx context.Context, te *TxEngine) (CoordConn, error) {
	return FakeCoordConn, nil
}

type fakeCoordConn struct {
}

func (f fakeCoordConn) Close() {

}

func (f fakeCoordConn) ResolveTransaction(ctx context.Context, dtid string) error {
	dtidCh <- dtid
	return nil
}
