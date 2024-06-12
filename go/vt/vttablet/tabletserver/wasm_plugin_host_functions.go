package tabletserver

import (
	"context"
	"unsafe"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/log"

	"github.com/tetratelabs/wazero/api"
)

//todo wasm: add a abstract layer, should not rely on 'wazero' and 'WazeroVM'.
//todo wasm: should not rely on tabletserver, should be a independent module. Now 'SetQueryHost' needs tabletserver.

func GetAbiVersionOnHost(ctx context.Context, mod api.Module, returnValuePtr uint32, returnValueSize uint32) uint32 {
	return uint32(copyHostStringIntoGuest(ctx, mod, AbiVersion, returnValuePtr, returnValueSize))
}

func GetRuntimeTypeOnHost(ctx context.Context, mod api.Module, returnValuePtr uint32, returnValueSize uint32) uint32 {
	return uint32(copyHostStringIntoGuest(ctx, mod, RuntimeType, returnValuePtr, returnValueSize))
}

func InfoLogOnHost(ctx context.Context, mod api.Module, ptr, size uint32) uint32 {
	bytes, ok := mod.Memory().Read(ptr, size)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	str := string(bytes)
	log.Info(str)
	return uint32(StatusOK)
}

func ErrorLogOnHost(ctx context.Context, mod api.Module, ptr, size uint32) uint32 {
	bytes, ok := mod.Memory().Read(ptr, size)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	str := string(bytes)
	log.Error(str)
	return uint32(StatusOK)
}

func SetGlobalValueByKeyOnHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroVM, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
	wazeroRuntime.mu.Lock()
	defer wazeroRuntime.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	valueBytes, ok := mod.Memory().Read(valuePtr, valueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}

	wazeroRuntime.hostSharedVariables[key] = valueBytes
	return uint32(StatusOK)
}

func GetGlobalValueByKeyOnHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroVM, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
	wazeroRuntime.mu.Lock()
	defer wazeroRuntime.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	_, exist := wazeroRuntime.hostSharedVariables[key]
	if !exist {
		return uint32(StatusNotFound)
	}
	return uint32(copyHostBytesIntoGuest(ctx, mod, wazeroRuntime.hostSharedVariables[key], returnValuePtr, returnValueSize))
}

func SetModuleValueByKeyOnHost(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.mu.Lock()
	defer module.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	valueBytes, ok := mod.Memory().Read(valuePtr, valueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}

	module.moduleSharingVariables[key] = valueBytes
	return uint32(StatusOK)
}

func GetModuleValueByKeyOnHost(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.mu.Lock()
	defer module.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	_, exist := module.moduleSharingVariables[key]
	if !exist {
		return uint32(StatusNotFound)
	}

	return uint32(copyHostBytesIntoGuest(ctx, mod, module.moduleSharingVariables[key], returnValuePtr, returnValueSize))
}

func GetQueryOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnValueData,
	returnValueSize uint32) uint32 {
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	return uint32(copyHostStringIntoGuest(ctx, mod, w.qre.query, returnValueData, returnValueSize))
}

func SetQueryOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryValuePtr, queryValueSize uint32) uint32 {
	bytes, ok := mod.Memory().Read(queryValuePtr, queryValueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	err := SetQueryToQre(w.qre, string(bytes))
	if err != nil {
		return uint32(StatusInternalFailure)
	}
	return uint32(StatusOK)
}

func GlobalLockOnHost(wazeroRuntime *WazeroVM) {
	wazeroRuntime.mu.Lock()
}

func GlobalUnLockOnHost(wazeroRuntime *WazeroVM) {
	wazeroRuntime.mu.Unlock()
}

func ModuleLockOnHost(hostModulePtr uint64) {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.moduleMu.Lock()
}

func ModuleUnLockOnHost(hostModulePtr uint64) {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.moduleMu.Unlock()
}

func SetErrorMessageOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, errMessagePtr, errMessageSize uint32) uint32 {
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	bytes, ok := mod.Memory().Read(errMessagePtr, errMessageSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	w.errorMessage = string(bytes)
	return uint32(StatusOK)
}

func GetErrorMessageOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnValueData, returnValueSize uint32) uint32 {
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	return uint32(copyHostStringIntoGuest(ctx, mod, w.errorMessage, returnValueData, returnValueSize))
}

func GetQueryResultOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnQueryResultPtr,
	returnQueryResultSize uint32) uint32 {
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))

	proto3Result := sqltypes.ResultToProto3(w.queryResult)
	bytes, err := proto3Result.MarshalVT()
	if err != nil {
		return uint32(StatusInternalFailure)
	}

	return uint32(copyHostBytesIntoGuest(ctx, mod, bytes, returnQueryResultPtr, returnQueryResultSize))
}

func SetQueryResultOnHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryResultPtr, queryResultSize uint32) uint32 {
	bytes, ok := mod.Memory().Read(queryResultPtr, queryResultSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}

	proto3Result := &querypb.QueryResult{}
	err := proto3Result.UnmarshalVT(bytes)
	if err != nil {
		return uint32(StatusInternalFailure)
	}
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	w.queryResult = sqltypes.Proto3ToResult(proto3Result)

	return uint32(StatusOK)
}
