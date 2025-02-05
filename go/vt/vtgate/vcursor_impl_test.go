/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"vitess.io/vitess/go/internal/global"

	"github.com/stretchr/testify/assert"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/proto/vschema"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ VSchemaOperator = (*fakeVSchemaOperator)(nil)

type fakeVSchemaOperator struct {
	vschema *vindexes.VSchema
}

func (f fakeVSchemaOperator) GetCurrentSrvVschema() *vschema.SrvVSchema {
	panic("implement me")
}

func (f fakeVSchemaOperator) UpdateVSchema(_ context.Context, _ string, _ *vschema.SrvVSchema) error {
	panic("implement me")
}

type fakeTopoServer struct {
}

// GetTopoServer returns the full topo.Server instance.
func (f *fakeTopoServer) GetTopoServer() (*topo.Server, error) {
	return nil, nil
}

// GetSrvKeyspaceNames returns the list of keyspaces served in
// the provided cell.
func (f *fakeTopoServer) GetSrvKeyspaceNames(_ context.Context, _ string, _ bool) ([]string, error) {
	return []string{"ks1"}, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (f *fakeTopoServer) GetSrvKeyspace(_ context.Context, _, _ string) (*topodatapb.SrvKeyspace, error) {
	zeroHexBytes, _ := hex.DecodeString("")
	eightyHexBytes, _ := hex.DecodeString("80")
	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{Name: "-80", KeyRange: &topodatapb.KeyRange{Start: zeroHexBytes, End: eightyHexBytes}},
					{Name: "80-", KeyRange: &topodatapb.KeyRange{Start: eightyHexBytes, End: zeroHexBytes}},
				},
			},
		},
	}
	return ks, nil
}

func (f *fakeTopoServer) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	ks, err := f.GetSrvKeyspace(ctx, cell, keyspace)
	callback(ks, err)
}

// WatchSrvVSchema starts watching the SrvVSchema object for
// the provided cell.  It will call the callback when
// a new value or an error occurs.
func (f *fakeTopoServer) WatchSrvVSchema(_ context.Context, _ string, _ func(*vschemapb.SrvVSchema, error) bool) {

}

func TestDestinationKeyspace(t *testing.T) {
	ks1 := &vindexes.Keyspace{
		Name:    "ks1",
		Sharded: false,
	}
	ks1Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks1,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	ks2 := &vindexes.Keyspace{
		Name:    "ks2",
		Sharded: false,
	}
	ks2Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks2,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
			ks2.Name: ks2Schema,
		}}

	vschemaWith1KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
		}}

	type testCase struct {
		vschema                 *vindexes.VSchema
		targetString, qualifier string
		expectedError           string
		expectedKeyspace        string
		expectedDest            key.Destination
		expectedTabletType      topodatapb.TabletType
	}

	tests := []testCase{{
		vschema:            vschemaWith1KS,
		targetString:       "ks1",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard(global.DefaultShard),
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard(global.DefaultShard),
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "ks1",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard(global.DefaultShard),
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2",
		qualifier:     "",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "",
		qualifier:     "ks2",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "",
		expectedError: errNoKeyspace.Error(),
	}}

	for i, tc := range tests {
		t.Run(strconv.Itoa(i)+tc.targetString, func(t *testing.T) {
			safeSession := NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString})
			impl, _ := newVCursorImpl(safeSession, "", sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false, querypb.ExecuteOptions_Gen4)
			var err error
			var mystmt sqlparser.Statement
			err = ResolveTabletType(safeSession, impl, mystmt, "")
			require.NoError(t, err)

			impl.vschema = tc.vschema
			dest, keyspace, _, err := impl.TargetDestination(tc.qualifier)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDest, dest)
				require.Equal(t, tc.expectedKeyspace, keyspace.Name)
				require.Equal(t, tc.expectedTabletType, impl.tabletType)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

var ks1 = &vindexes.Keyspace{Name: "ks1"}
var ks1Schema = &vindexes.KeyspaceSchema{Keyspace: ks1}
var ks2 = &vindexes.Keyspace{Name: "ks2"}
var ks2Schema = &vindexes.KeyspaceSchema{Keyspace: ks2}
var vschemaWith1KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
	},
}
var vschemaWith2KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
		ks2.Name: ks2Schema,
	}}

func TestSetTarget(t *testing.T) {
	type testCase struct {
		vschema       *vindexes.VSchema
		targetString  string
		expectedError string
	}

	tests := []testCase{{
		vschema:      vschemaWith2KS,
		targetString: "",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks1",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks2",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks3",
		expectedError: "VT05003: unknown database 'ks3' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks2@replica",
		expectedError: "can't execute the given command because you have an active transaction",
	}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{InTransaction: true}), "", sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false, querypb.ExecuteOptions_Gen4)
			vc.vschema = tc.vschema
			err := vc.SetTarget(tc.targetString, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, vc.safeSession.TargetString, tc.targetString)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestPlanPrefixKey(t *testing.T) {
	type testCase struct {
		vschema               *vindexes.VSchema
		targetString          string
		expectedPlanPrefixKey string
	}

	tests := []testCase{{
		vschema:               vschemaWith1KS,
		targetString:          "",
		expectedPlanPrefixKey: "@primaryDestinationShard(0)",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1@replica",
		expectedPlanPrefixKey: "ks1@replicaDestinationShard(0)",
	}}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			ss := NewSafeSession(&vtgatepb.Session{InTransaction: false})
			ss.SetTargetString(tc.targetString)
			vc, err := newVCursorImpl(ss, "", sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false, querypb.ExecuteOptions_Gen4)
			require.NoError(t, err)
			vc.vschema = tc.vschema
			var mystmt sqlparser.Statement
			err = ResolveTabletType(ss, vc, mystmt, "")
			require.NoError(t, err)
			require.Equal(t, tc.expectedPlanPrefixKey, vc.planPrefixKey(context.Background()))
		})
	}
}

func TestFirstSortedKeyspace(t *testing.T) {
	ks1Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "xks1"}}
	ks2Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks2"}}
	ks3Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks1"}}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1Schema.Keyspace.Name: ks1Schema,
			ks2Schema.Keyspace.Name: ks2Schema,
			ks3Schema.Keyspace.Name: ks3Schema,
		}}

	vc, err := newVCursorImpl(NewSafeSession(nil), "", sqlparser.MarginComments{}, nil, nil, &fakeVSchemaOperator{vschema: vschemaWith2KS}, vschemaWith2KS, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false, querypb.ExecuteOptions_Gen4)
	require.NoError(t, err)
	ks, err := vc.FirstSortedKeyspace()
	require.NoError(t, err)
	require.Equal(t, ks3Schema.Keyspace, ks)
}

func Test_vcursorImpl_DefaultKeyspace(t *testing.T) {
	defaultKs := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "mysql"}}
	ks1Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "xks1"}}
	ks2Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks2"}}
	ks3Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks1"}}
	type fields struct {
		vschema *vindexes.VSchema
	}
	tests := []struct {
		name    string
		fields  fields
		want    *vindexes.Keyspace
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no default keyspace",
			fields: fields{
				vschema: &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						"mysql":                 defaultKs,
						ks1Schema.Keyspace.Name: ks1Schema,
						ks2Schema.Keyspace.Name: ks2Schema,
						ks3Schema.Keyspace.Name: ks3Schema,
					}},
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &vcursorImpl{
				vschema: tt.fields.vschema,
			}
			got, err := vc.DefaultKeyspace()
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equalf(t, tt.want, got, "DefaultKeyspace()")
		})
	}
}
