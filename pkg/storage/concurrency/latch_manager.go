// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import "github.com/cockroachdb/cockroach/pkg/storage/spanlatch"

// latchManagerImpl implements the latchManager interface.
//
// WIP: In reality, I think we would pull in pkg/storage/spanlatch into a new
//      pkg/storage/concurrency/latch package.
type latchManagerImpl struct {
	m spanlatch.Manager
}

func (m *latchManagerImpl) acquire(req Request) (latchGuard, error) {
	return m.m.Acquire(req.context(), req.spans())
}

func (m *latchManagerImpl) release(lg latchGuard) {
	m.m.Release(lg.(*spanlatch.Guard))
}
