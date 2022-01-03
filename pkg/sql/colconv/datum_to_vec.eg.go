// Code generated by execgen; DO NOT EDIT.
// Copyright 2020 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colconv

import (
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ encoding.Direction
	_ = typeconv.DatumVecCanonicalTypeFamily
)

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type. Note that the signature of the
// return function doesn't contain an error since we assume that the conversion
// must succeed. If for some reason it fails, a panic will be emitted and will
// be caught by the panic-catcher mechanism of the vectorized engine and will
// be propagated as an error accordingly.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) interface{} {
	switch ct.Family() {
	case types.BoolFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return bool(*datum.(*tree.DBool))
			}
		}
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return func(datum tree.Datum) interface{} {

				return int16(*datum.(*tree.DInt))
			}
		case 32:
			return func(datum tree.Datum) interface{} {

				return int32(*datum.(*tree.DInt))
			}
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return int64(*datum.(*tree.DInt))
			}
		}
	case types.FloatFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return float64(*datum.(*tree.DFloat))
			}
		}
	case types.DecimalFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return &datum.(*tree.DDecimal).Decimal
			}
		}
	case types.DateFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DDate).UnixEpochDaysWithOrig()
			}
		}
	case types.TimestampFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DTimestamp).Time
			}
		}
	case types.IntervalFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DInterval).Duration
			}
		}
	case types.StringFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {
				// Handle other STRING-related OID types, like oid.T_name.
				wrapper, ok := datum.(*tree.DOidWrapper)
				if ok {
					datum = wrapper.Wrapped
				}
				return encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DString)))
			}
		}
	case types.BytesFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DBytes)))
			}
		}
	case types.TimestampTZFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DTimestampTZ).Time
			}
		}
	case types.UuidFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DUuid).UUID.GetBytesMut()
			}
		}
	case types.JsonFamily:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum.(*tree.DJSON).JSON
			}
		}
	case typeconv.DatumVecCanonicalTypeFamily:
	default:
		switch ct.Width() {
		case -1:
		default:
			return func(datum tree.Datum) interface{} {

				return datum
			}
		}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly unhandled type %s", ct.DebugString()))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
