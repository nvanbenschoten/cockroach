// Copyright 2018 The Cockroach Authors.
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

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// txnMetrics is a txnInterceptor in charge of updating metrics about the
// behavior and outcome of a transaction. It records information about the
// requests that a transaction sends and updates counters and histograms when
// the transaction completes.
//
// TODO(nvanbenschoten): Rename to txnMetricRecorder.
// TODO(nvanbenschoten): Unit test this file.
type txnMetrics struct {
	wrapped lockedSender
	metrics *TxnMetrics
	clock   *hlc.Clock

	txn           *roachpb.Transaction
	txnStartNanos int64
	onePCCommit   bool
}

// SendLocked is part of the txnInterceptor interface.
func (m *txnMetrics) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	m.onePCCommit = ba.IsCompleteTransaction()

	if m.txnStartNanos == 0 {
		m.txnStartNanos = timeutil.Now().UnixNano()
	}

	return m.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (m *txnMetrics) setWrapped(wrapped lockedSender) { m.wrapped = wrapped }

// populateMetaLocked is part of the txnInterceptor interface.
func (*txnMetrics) populateMetaLocked(*roachpb.TxnCoordMeta) {}

// augmentMetaLocked is part of the txnInterceptor interface.
func (*txnMetrics) augmentMetaLocked(roachpb.TxnCoordMeta) {}

// epochBumpedLocked is part of the txnInterceptor interface.
func (*txnMetrics) epochBumpedLocked() {}

// closeLocked is part of the txnInterceptor interface.
func (m *txnMetrics) closeLocked() {
	if m.onePCCommit {
		m.metrics.Commits1PC.Inc(1)
	}

	if m.txnStartNanos != 0 {
		duration := timeutil.Now().UnixNano() - m.txnStartNanos
		if duration >= 0 {
			m.metrics.Durations.RecordValue(duration)
		}
	}
	restarts := int64(m.txn.Epoch)
	status := m.txn.Status

	// TODO(andrei): We only record txn that had any restarts, otherwise the
	// serialization induced by the histogram shows on profiles. Figure something
	// out to make it cheaper - maybe augment the histogram library with an
	// "expected value" that is a cheap counter for the common case. See #30644.
	// Also, the epoch is not currently an accurate count since we sometimes bump
	// it artificially (in the parallel execution queue).
	if restarts > 0 {
		m.metrics.Restarts.RecordValue(restarts)
	}
	switch status {
	case roachpb.PENDING:
		// NOTE(andrei): Getting a PENDING status here is possible when this
		// interceptor is closed without a rollback ever succeeding.
		// We increment the Aborts metric nevertheless; not sure how these
		// transactions should be accounted.
		m.metrics.Aborts.Inc(1)
	case roachpb.STAGING:
		// This should never be possible, as the STAGING status should never
		// be propagated above the txnCommitter.
		ctx := context.Background()
		log.Warningf(ctx, "txnMetrics.closeLocked called with a STAGING txn: %v", m.txn)
	case roachpb.ABORTED:
		m.metrics.Aborts.Inc(1)
	case roachpb.COMMITTED:
		// Note that successful read-only txn are also counted as committed, even
		// though they never had a txn record.
		m.metrics.Commits.Inc(1)
	}
}
