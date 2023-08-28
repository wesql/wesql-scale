/*
Copyright 2021 The Vitess Authors.

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
// Code generated by Sizegen. DO NOT EDIT.

package tabletserver

import hack "vitess.io/vitess/go/hack"

func (cached *TabletPlan) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(112)
	}
	// field Plan *vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder.Plan
	size += cached.Plan.CachedSize(true)
	// field Original string
	size += hack.RuntimeAllocSize(int64(len(cached.Original)))
	// field Rules *vitess.io/vitess/go/vt/vttablet/tabletserver/rules.Rules
	size += cached.Rules.CachedSize(true)
	// field Authorized [][]*vitess.io/vitess/go/vt/tableacl.ACLResult
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.Authorized)) * int64(24))
		for _, elem := range cached.Authorized {
			{
				size += hack.RuntimeAllocSize(int64(cap(elem)) * int64(8))
				for _, elem := range elem {
					size += elem.CachedSize(true)
				}
			}
		}
	}
	return size
}
