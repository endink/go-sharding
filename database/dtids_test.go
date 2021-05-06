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

package database

import (
	"github.com/endink/go-sharding/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDTID(t *testing.T) {
	in := &DbSession{
		Target: &Target{
			Schema:     "aa",
			DataSource: "0",
			TabletType: TabletTypeMaster,
		},
		TransactionId: 1,
	}
	dtid := Dtid(in)
	want := "aa:0:1"
	if dtid != want {
		t.Errorf("generateDTID: %s, want %s", dtid, want)
	}
	out, err := NewDbSession(dtid)
	require.NoError(t, err)
	eq, err := util.CheckJsonEqual(in, out)

	require.NoError(t, err)

	if !eq {
		t.Errorf("DbSession: %+v, want %+v", out, in)
	}
	_, err = NewDbSession("badParts")
	want = "invalid parts in dtid: badParts"
	if err == nil || err.Error() != want {
		t.Errorf("DbSession(\"badParts\"): %v, want %s", err, want)
	}
	_, err = NewDbSession("a:b:badid")
	want = "invalid transaction id in dtid: a:b:badid"
	if err == nil || err.Error() != want {
		t.Errorf("DbSession(\"a:b:badid\"): %v, want %s", err, want)
	}
}

func TestTransactionID(t *testing.T) {
	out, err := TransactionID("aa:0:1")
	require.NoError(t, err)
	if out != 1 {
		t.Errorf("TransactionID(aa:0:1): %d, want 1", out)
	}
	_, err = TransactionID("badParts")
	want := "invalid parts in dtid: badParts"
	if err == nil || err.Error() != want {
		t.Errorf("TransactionID(\"badParts\"): %v, want %s", err, want)
	}
	_, err = TransactionID("a:b:badid")
	want = "invalid transaction id in dtid: a:b:badid"
	if err == nil || err.Error() != want {
		t.Errorf("TransactionID(\"a:b:badid\"): %v, want %s", err, want)
	}
}
