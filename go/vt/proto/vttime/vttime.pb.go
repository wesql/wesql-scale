//
//Copyright 2019 The Vitess Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// This package contains a shared time data structure

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v3.21.3
// source: vttime.proto

package vttime

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Time represents a time stamp in nanoseconds. In go, use logutil library
// to convert times.
type Time struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Seconds     int64 `protobuf:"varint,1,opt,name=seconds,proto3" json:"seconds,omitempty"`
	Nanoseconds int32 `protobuf:"varint,2,opt,name=nanoseconds,proto3" json:"nanoseconds,omitempty"`
}

func (x *Time) Reset() {
	*x = Time{}
	if protoimpl.UnsafeEnabled {
		mi := &file_vttime_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Time) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Time) ProtoMessage() {}

func (x *Time) ProtoReflect() protoreflect.Message {
	mi := &file_vttime_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Time.ProtoReflect.Descriptor instead.
func (*Time) Descriptor() ([]byte, []int) {
	return file_vttime_proto_rawDescGZIP(), []int{0}
}

func (x *Time) GetSeconds() int64 {
	if x != nil {
		return x.Seconds
	}
	return 0
}

func (x *Time) GetNanoseconds() int32 {
	if x != nil {
		return x.Nanoseconds
	}
	return 0
}

type Duration struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Seconds int64 `protobuf:"varint,1,opt,name=seconds,proto3" json:"seconds,omitempty"`
	Nanos   int32 `protobuf:"varint,2,opt,name=nanos,proto3" json:"nanos,omitempty"`
}

func (x *Duration) Reset() {
	*x = Duration{}
	if protoimpl.UnsafeEnabled {
		mi := &file_vttime_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Duration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Duration) ProtoMessage() {}

func (x *Duration) ProtoReflect() protoreflect.Message {
	mi := &file_vttime_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Duration.ProtoReflect.Descriptor instead.
func (*Duration) Descriptor() ([]byte, []int) {
	return file_vttime_proto_rawDescGZIP(), []int{1}
}

func (x *Duration) GetSeconds() int64 {
	if x != nil {
		return x.Seconds
	}
	return 0
}

func (x *Duration) GetNanos() int32 {
	if x != nil {
		return x.Nanos
	}
	return 0
}

var File_vttime_proto protoreflect.FileDescriptor

var file_vttime_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x76, 0x74, 0x74, 0x69, 0x6d, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x76, 0x74, 0x74, 0x69, 0x6d, 0x65, 0x22, 0x42, 0x0a, 0x04, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x18,
	0x0a, 0x07, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x07, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x12, 0x20, 0x0a, 0x0b, 0x6e, 0x61, 0x6e, 0x6f,
	0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x6e,
	0x61, 0x6e, 0x6f, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x3a, 0x0a, 0x08, 0x44, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73,
	0x12, 0x14, 0x0a, 0x05, 0x6e, 0x61, 0x6e, 0x6f, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x05, 0x6e, 0x61, 0x6e, 0x6f, 0x73, 0x42, 0x25, 0x5a, 0x23, 0x76, 0x69, 0x74, 0x65, 0x73, 0x73,
	0x2e, 0x69, 0x6f, 0x2f, 0x76, 0x69, 0x74, 0x65, 0x73, 0x73, 0x2f, 0x67, 0x6f, 0x2f, 0x76, 0x74,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x76, 0x74, 0x74, 0x69, 0x6d, 0x65, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_vttime_proto_rawDescOnce sync.Once
	file_vttime_proto_rawDescData = file_vttime_proto_rawDesc
)

func file_vttime_proto_rawDescGZIP() []byte {
	file_vttime_proto_rawDescOnce.Do(func() {
		file_vttime_proto_rawDescData = protoimpl.X.CompressGZIP(file_vttime_proto_rawDescData)
	})
	return file_vttime_proto_rawDescData
}

var file_vttime_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_vttime_proto_goTypes = []any{
	(*Time)(nil),     // 0: vttime.Time
	(*Duration)(nil), // 1: vttime.Duration
}
var file_vttime_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_vttime_proto_init() }
func file_vttime_proto_init() {
	if File_vttime_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_vttime_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Time); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_vttime_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*Duration); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_vttime_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_vttime_proto_goTypes,
		DependencyIndexes: file_vttime_proto_depIdxs,
		MessageInfos:      file_vttime_proto_msgTypes,
	}.Build()
	File_vttime_proto = out.File
	file_vttime_proto_rawDesc = nil
	file_vttime_proto_goTypes = nil
	file_vttime_proto_depIdxs = nil
}
