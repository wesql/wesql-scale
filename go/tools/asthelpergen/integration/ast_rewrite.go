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
// Code generated by ASTHelperGen. DO NOT EDIT.

package integration

func (a *application) rewriteAST(parent AST, node AST, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	switch node := node.(type) {
	case BasicType:
		return a.rewriteBasicType(parent, node, replacer)
	case Bytes:
		return a.rewriteBytes(parent, node, replacer)
	case InterfaceContainer:
		return a.rewriteInterfaceContainer(parent, node, replacer)
	case InterfaceSlice:
		return a.rewriteInterfaceSlice(parent, node, replacer)
	case *Leaf:
		return a.rewriteRefOfLeaf(parent, node, replacer)
	case LeafSlice:
		return a.rewriteLeafSlice(parent, node, replacer)
	case *NoCloneType:
		return a.rewriteRefOfNoCloneType(parent, node, replacer)
	case *RefContainer:
		return a.rewriteRefOfRefContainer(parent, node, replacer)
	case *RefSliceContainer:
		return a.rewriteRefOfRefSliceContainer(parent, node, replacer)
	case *SubImpl:
		return a.rewriteRefOfSubImpl(parent, node, replacer)
	case ValueContainer:
		return a.rewriteValueContainer(parent, node, replacer)
	case ValueSliceContainer:
		return a.rewriteValueSliceContainer(parent, node, replacer)
	default:
		// this should never happen
		return true
	}
}
func (a *application) rewriteBytes(parent AST, node Bytes, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteInterfaceContainer(parent AST, node InterfaceContainer, replacer replacerFunc) bool {
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteInterfaceSlice(parent AST, node InterfaceSlice, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	for x, el := range node {
		if !a.rewriteAST(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(InterfaceSlice)[idx] = newNode.(AST)
			}
		}(x)) {
			return false
		}
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfLeaf(parent AST, node *Leaf, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteLeafSlice(parent AST, node LeafSlice, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	for x, el := range node {
		if !a.rewriteRefOfLeaf(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(LeafSlice)[idx] = newNode.(*Leaf)
			}
		}(x)) {
			return false
		}
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfNoCloneType(parent AST, node *NoCloneType, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfRefContainer(parent AST, node *RefContainer, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if !a.rewriteAST(node, node.ASTType, func(newNode, parent AST) {
		parent.(*RefContainer).ASTType = newNode.(AST)
	}) {
		return false
	}
	if !a.rewriteRefOfLeaf(node, node.ASTImplementationType, func(newNode, parent AST) {
		parent.(*RefContainer).ASTImplementationType = newNode.(*Leaf)
	}) {
		return false
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfRefSliceContainer(parent AST, node *RefSliceContainer, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	for x, el := range node.ASTElements {
		if !a.rewriteAST(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(*RefSliceContainer).ASTElements[idx] = newNode.(AST)
			}
		}(x)) {
			return false
		}
	}
	for x, el := range node.ASTImplementationElements {
		if !a.rewriteRefOfLeaf(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(*RefSliceContainer).ASTImplementationElements[idx] = newNode.(*Leaf)
			}
		}(x)) {
			return false
		}
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfSubImpl(parent AST, node *SubImpl, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if !a.rewriteSubIface(node, node.inner, func(newNode, parent AST) {
		parent.(*SubImpl).inner = newNode.(SubIface)
	}) {
		return false
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteValueContainer(parent AST, node ValueContainer, replacer replacerFunc) bool {
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if !a.rewriteAST(node, node.ASTType, func(newNode, parent AST) {
		panic("[BUG] tried to replace 'ASTType' on 'ValueContainer'")
	}) {
		return false
	}
	if !a.rewriteRefOfLeaf(node, node.ASTImplementationType, func(newNode, parent AST) {
		panic("[BUG] tried to replace 'ASTImplementationType' on 'ValueContainer'")
	}) {
		return false
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteValueSliceContainer(parent AST, node ValueSliceContainer, replacer replacerFunc) bool {
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	for _, el := range node.ASTElements {
		if !a.rewriteAST(node, el, func(newNode, parent AST) {
			panic("[BUG] tried to replace 'ASTElements' on 'ValueSliceContainer'")
		}) {
			return false
		}
	}
	for _, el := range node.ASTImplementationElements {
		if !a.rewriteRefOfLeaf(node, el, func(newNode, parent AST) {
			panic("[BUG] tried to replace 'ASTImplementationElements' on 'ValueSliceContainer'")
		}) {
			return false
		}
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteSubIface(parent AST, node SubIface, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	switch node := node.(type) {
	case *SubImpl:
		return a.rewriteRefOfSubImpl(parent, node, replacer)
	default:
		// this should never happen
		return true
	}
}
func (a *application) rewriteBasicType(parent AST, node BasicType, replacer replacerFunc) bool {
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfInterfaceContainer(parent AST, node *InterfaceContainer, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if a.post != nil {
		if a.pre == nil {
			a.cur.replacer = replacer
			a.cur.parent = parent
			a.cur.node = node
		}
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfValueContainer(parent AST, node *ValueContainer, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	if !a.rewriteAST(node, node.ASTType, func(newNode, parent AST) {
		parent.(*ValueContainer).ASTType = newNode.(AST)
	}) {
		return false
	}
	if !a.rewriteRefOfLeaf(node, node.ASTImplementationType, func(newNode, parent AST) {
		parent.(*ValueContainer).ASTImplementationType = newNode.(*Leaf)
	}) {
		return false
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
func (a *application) rewriteRefOfValueSliceContainer(parent AST, node *ValueSliceContainer, replacer replacerFunc) bool {
	if node == nil {
		return true
	}
	if a.pre != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.pre(&a.cur) {
			return true
		}
	}
	for x, el := range node.ASTElements {
		if !a.rewriteAST(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(*ValueSliceContainer).ASTElements[idx] = newNode.(AST)
			}
		}(x)) {
			return false
		}
	}
	for x, el := range node.ASTImplementationElements {
		if !a.rewriteRefOfLeaf(node, el, func(idx int) replacerFunc {
			return func(newNode, parent AST) {
				parent.(*ValueSliceContainer).ASTImplementationElements[idx] = newNode.(*Leaf)
			}
		}(x)) {
			return false
		}
	}
	if a.post != nil {
		a.cur.replacer = replacer
		a.cur.parent = parent
		a.cur.node = node
		if !a.post(&a.cur) {
			return false
		}
	}
	return true
}
