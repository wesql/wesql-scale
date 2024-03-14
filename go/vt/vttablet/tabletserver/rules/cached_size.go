/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

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

package rules

import hack "vitess.io/vitess/go/hack"

type cachedObject interface {
	CachedSize(alloc bool) int64
}

func (cached *BindVarCond) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(48)
	}
	// field name string
	size += hack.RuntimeAllocSize(int64(len(cached.name)))
	// field value vitess.io/vitess/go/vt/vttablet/tabletserver/rules.bvcValue
	if cc, ok := cached.value.(cachedObject); ok {
		size += cc.CachedSize(true)
	}
	return size
}
func (cached *Rule) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(256)
	}
	// field Description string
	size += hack.RuntimeAllocSize(int64(len(cached.Description)))
	// field Name string
	size += hack.RuntimeAllocSize(int64(len(cached.Name)))
	//todo earayu: where is Priority?

	// field plans []vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder.PlanType
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.plans)) * int64(8))
	}
	// field tableNames []string
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.tableNames)) * int64(16))
		for _, elem := range cached.tableNames {
			size += hack.RuntimeAllocSize(int64(len(elem)))
		}
	}
	// field query vitess.io/vitess/go/vt/vttablet/tabletserver/rules.namedRegexp
	size += cached.query.CachedSize(false)
	// field requestIP vitess.io/vitess/go/vt/vttablet/tabletserver/rules.namedRegexp
	size += cached.requestIP.CachedSize(false)
	// field user vitess.io/vitess/go/vt/vttablet/tabletserver/rules.namedRegexp
	size += cached.user.CachedSize(false)
	// field leadingComment vitess.io/vitess/go/vt/vttablet/tabletserver/rules.namedRegexp
	size += cached.leadingComment.CachedSize(false)
	// field trailingComment vitess.io/vitess/go/vt/vttablet/tabletserver/rules.namedRegexp
	size += cached.trailingComment.CachedSize(false)
	// field bindVarConds []vitess.io/vitess/go/vt/vttablet/tabletserver/rules.BindVarCond
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.bindVarConds)) * int64(48))
		for _, elem := range cached.bindVarConds {
			size += elem.CachedSize(false)
		}
	}
	return size
}
func (cached *Rules) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field rules []*vitess.io/vitess/go/vt/vttablet/tabletserver/rules.Rule
	{
		size += hack.RuntimeAllocSize(int64(cap(cached.rules)) * int64(8))
		for _, elem := range cached.rules {
			size += elem.CachedSize(true)
		}
	}
	return size
}
func (cached *bvcre) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(8)
	}
	// field re *regexp.Regexp
	if cached.re != nil {
		size += hack.RuntimeAllocSize(int64(153))
	}
	return size
}
func (cached *namedRegexp) CachedSize(alloc bool) int64 {
	if cached == nil {
		return int64(0)
	}
	size := int64(0)
	if alloc {
		size += int64(24)
	}
	// field name string
	size += hack.RuntimeAllocSize(int64(len(cached.name)))
	// field Regexp *regexp.Regexp
	if cached.Regexp != nil {
		size += hack.RuntimeAllocSize(int64(153))
	}
	return size
}
