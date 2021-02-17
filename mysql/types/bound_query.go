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

package types

// BoundQuery is a query with its bind variables
type BoundQuery struct {
	// sql is the SQL query to execute
	Sql string
	// bind_variables is a map of all bind variables to expand in the query.
	// nil values are not allowed. Use NULL_TYPE to express a NULL value.
	BindVariables map[string]*BindVariable
}
