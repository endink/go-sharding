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

package planner

import (
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/testkit"
	"reflect"
	"testing"
)

func TestBuildPermissions(t *testing.T) {
	tcases := []struct {
		input  string
		output []core.Permission
	}{{
		input: "select * from t",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleReader,
		}},
	}, {
		input: "select * from t1 union select * from t2",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleReader,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "insert into t values()",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleWriter,
		}},
	}, {
		input: "update t set a=1",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleWriter,
		}},
	}, {
		input: "delete from t",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleWriter,
		}},
	}, {
		input:  "set a=1",
		output: nil,
	}, {
		input:  "show variable like 'a%'",
		output: nil,
	}, {
		input:  "describe select * from t",
		output: nil,
	}, {
		input: "create table t",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleAdmin,
		}},
	}, {
		input: "rename table t1 to t2",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleAdmin,
		}, {
			TableName: "t2",
			Role:      core.RoleAdmin,
		}},
	}, {
		input: "flush tables t1, t2",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleAdmin,
		}, {
			TableName: "t2",
			Role:      core.RoleAdmin,
		}},
	}, {
		input: "drop table t",
		output: []core.Permission{{
			TableName: "t",
			Role:      core.RoleAdmin,
		}},
	}, {
		input:  "repair t",
		output: nil,
	}, {
		input: "select (select a from t2) from t1",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleReader,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "insert into t1 values((select a from t2), 1)",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleWriter,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "update t1 set a = (select b from t2)",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleWriter,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "delete from t1 where a = (select b from t2)",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleWriter,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "select * from t1, t2",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleReader,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "select * from (t1, t2)",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleReader,
		}, {
			TableName: "t2",
			Role:      core.RoleReader,
		}},
	}, {
		input: "update t1 join t2 on a=b set c=d",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleWriter,
		}, {
			TableName: "t2",
			Role:      core.RoleWriter,
		}},
	}, {
		input: "update (select * from t1) as a join t2 on a=b set c=d",
		output: []core.Permission{{
			TableName: "t1",
			Role:      core.RoleWriter,
		}, {
			TableName: "t2",
			Role:      core.RoleWriter,
		}},
	}}

	for _, tcase := range tcases {
		stmt := testkit.ParseForTest(tcase.input, t)
		got := BuildPermissions(stmt)
		if !reflect.DeepEqual(got, tcase.output) {
			t.Errorf("BuildPermissions(%s): %v, want %v", tcase.input, got, tcase.output)
		}
	}
}
