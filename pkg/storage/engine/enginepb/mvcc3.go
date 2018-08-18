// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package enginepb

import (
	"fmt"

	proto "github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCStatsDelta) ToStats() MVCCStats {
	return MVCCStats{
		ContainsEstimates:  ms.ContainsEstimates,
		LastUpdateNanos:    ms.LastUpdateNanos,
		IntentAge:          ms.IntentAge,
		GCBytesAge:         ms.GCBytesAge,
		LiveBytes:          ms.LiveBytes,
		LiveCount:          ms.LiveCount,
		KeyBytes:           ms.KeyBytes,
		KeyCount:           ms.KeyCount,
		ValBytes:           ms.ValBytes,
		ValCount:           ms.ValCount,
		IntentBytes:        ms.IntentBytes,
		IntentCount:        ms.IntentCount,
		SysBytes:           ms.SysBytes,
		SysCount:           ms.SysCount,
		WriteHighWatermark: FixedTimestamp(ms.WriteHighWatermark),
	}
}

// ToStatsDelta converts the receiver to an MVCCStatsDelta.
func (ms *MVCCStats) ToStatsDelta() MVCCStatsDelta {
	return MVCCStatsDelta{
		ContainsEstimates:  ms.ContainsEstimates,
		LastUpdateNanos:    ms.LastUpdateNanos,
		IntentAge:          ms.IntentAge,
		GCBytesAge:         ms.GCBytesAge,
		LiveBytes:          ms.LiveBytes,
		LiveCount:          ms.LiveCount,
		KeyBytes:           ms.KeyBytes,
		KeyCount:           ms.KeyCount,
		ValBytes:           ms.ValBytes,
		ValCount:           ms.ValCount,
		IntentBytes:        ms.IntentBytes,
		IntentCount:        ms.IntentCount,
		SysBytes:           ms.SysBytes,
		SysCount:           ms.SysCount,
		WriteHighWatermark: hlc.Timestamp(ms.WriteHighWatermark),
	}
}

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() MVCCStats {
	return MVCCStats{
		ContainsEstimates:  ms.ContainsEstimates,
		LastUpdateNanos:    ms.LastUpdateNanos,
		IntentAge:          ms.IntentAge,
		GCBytesAge:         ms.GCBytesAge,
		LiveBytes:          ms.LiveBytes,
		LiveCount:          ms.LiveCount,
		KeyBytes:           ms.KeyBytes,
		KeyCount:           ms.KeyCount,
		ValBytes:           ms.ValBytes,
		ValCount:           ms.ValCount,
		IntentBytes:        ms.IntentBytes,
		IntentCount:        ms.IntentCount,
		SysBytes:           ms.SysBytes,
		SysCount:           ms.SysCount,
		WriteHighWatermark: FixedTimestamp(ms.WriteHighWatermark),
	}
}

// ToPersistentStats converts the receiver to an MVCCPersistentStats.
func (ms *MVCCStats) ToPersistentStats() MVCCPersistentStats {
	return MVCCPersistentStats{
		ContainsEstimates:  ms.ContainsEstimates,
		LastUpdateNanos:    ms.LastUpdateNanos,
		IntentAge:          ms.IntentAge,
		GCBytesAge:         ms.GCBytesAge,
		LiveBytes:          ms.LiveBytes,
		LiveCount:          ms.LiveCount,
		KeyBytes:           ms.KeyBytes,
		KeyCount:           ms.KeyCount,
		ValBytes:           ms.ValBytes,
		ValCount:           ms.ValCount,
		IntentBytes:        ms.IntentBytes,
		IntentCount:        ms.IntentCount,
		SysBytes:           ms.SysBytes,
		SysCount:           ms.SysCount,
		WriteHighWatermark: hlc.Timestamp(ms.WriteHighWatermark),
	}
}

// Forward updates the timestamp from the one given, if that moves it forwards
// in time. Returns true if the timestamp was adjusted and false otherwise.
func (t *FixedTimestamp) Forward(s hlc.Timestamp) bool {
	tt := hlc.Timestamp(*t)
	if tt.Less(s) {
		*t = FixedTimestamp(s)
		return true
	}
	return false
}

var isolationTypeLowerCase = map[int32]string{
	0: "serializable",
	1: "snapshot",
}

// ToLowerCaseString returns the lower case version of String().
// Asking for lowercase is common enough (pg_setting / SHOW in SQL)
// that we don't want to call strings.ToLower(x.String()) all the time.
func (x IsolationType) ToLowerCaseString() string {
	return proto.EnumName(isolationTypeLowerCase, int32(x))
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (op *MVCCLogicalOp) MustSetValue(value interface{}) {
	op.Reset()
	if !op.SetValue(value) {
		panic(fmt.Sprintf("%T excludes %T", op, value))
	}
}
