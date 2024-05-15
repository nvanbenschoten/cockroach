// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttest

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// Handle is the entrypoint for data-driven interaction testing. Commands and
// parameters are parsed from the supplied TestData. Errors during data parsing
// are reported via the supplied *testing.T; errors from the raft nodes and the
// storage engine are reported to the output buffer.
func (env *InteractionEnv) Handle(t *testing.T, d datadriven.TestData) string {
	env.Output.Reset()
	var err error
	switch d.Cmd {
	case "store-liveness":
		// Print global store liveness state.
		//
		// Example:
		//
		// store-liveness
		_, err = env.Output.WriteString(env.fabric.String())
	case "bump-epoch":
		err = env.handleBumpEpoch(t, d)
	case "bump-support-for":
		// Bumps support for a remote store.
		//
		// Example:
		//
		// bump-support-for 1 2
		// bumps support for store 1 from the perspective of store 2.
		err = env.handleBumpSupport(t, d)
	case "withdraw-support-for":
		// Withdraws support for a remote store. The epoch is left un-altered. The
		// caller may bump support for the remote store to bump the epoch and start
		// providing support at the higher epoch.
		//
		// Example:
		//
		// withdraw-support-for 1 2
		// withdraws support for store 1 from the perspective of store 2.
		err = env.handleWithdrawSupport(t, d)
	case "_breakpoint":
		// This is a helper case to attach a debugger to when a problem needs
		// to be investigated in a longer test file. In such a case, add the
		// following stanza immediately before the interesting behavior starts:
		//
		// _breakpoint:
		// ----
		// ok
		//
		// and set a breakpoint on the `case` above.
	case "add-nodes":
		// Example:
		//
		// add-nodes <number-of-nodes-to-add> voters=(1 2 3) learners=(4 5) index=2 content=foo async-storage-writes=true
		err = env.handleAddNodes(t, d)
	case "campaign":
		// Example:
		//
		// campaign <id-of-candidate>
		err = env.handleCampaign(t, d)
	case "compact":
		// Example:
		//
		// compact <id> <new-first-index>
		err = env.handleCompact(t, d)
	case "deliver-msgs":
		// Deliver the messages for a given recipient.
		//
		// Example:
		//
		// deliver-msgs <idx> type=MsgApp drop=(2,3)
		err = env.handleDeliverMsgs(t, d)
	case "process-ready":
		// Example:
		//
		// process-ready 3
		err = env.handleProcessReady(t, d)
	case "process-append-thread":
		// Example:
		//
		// process-append-thread 3
		err = env.handleProcessAppendThread(t, d)
	case "process-apply-thread":
		// Example:
		//
		// process-apply-thread 3
		err = env.handleProcessApplyThread(t, d)
	case "log-level":
		// Set the log level. NONE disables all output, including from the test
		// harness (except errors).
		//
		// Example:
		//
		// log-level WARN
		err = env.handleLogLevel(d)
	case "raft-log":
		// Print the Raft log.
		//
		// Example:
		//
		// raft-log 3
		err = env.handleRaftLog(t, d)
	case "raft-state":
		// Print Raft state of all nodes (whether the node is leading,
		// following, etc.). The information for node n is based on
		// n's view.
		err = env.handleRaftState()
	case "set-randomized-election-timeout":
		// Set the randomized election timeout for the given node. Will be reset
		// again when the node changes state.
		//
		// Example:
		//
		// set-randomized-election-timeout 1 timeout=5
		err = env.handleSetRandomizedElectionTimeout(t, d)
	case "stabilize":
		// Deliver messages to and run process-ready on the set of IDs until
		// no more work is to be done. If no ids are given, all nodes are used.
		//
		// Example:
		//
		// stabilize 1 4
		err = env.handleStabilize(t, d)
	case "status":
		// Print Raft status.
		//
		// Example:
		//
		// status 5
		err = env.handleStatus(t, d)
	case "tick-election":
		// Tick an election timeout interval for the given node (but beware the
		// randomized timeout).
		//
		// Example:
		//
		// tick-election 3
		err = env.handleTickElection(t, d)
	case "tick-heartbeat":
		// Tick a heartbeat interval.
		//
		// Example:
		//
		// tick-heartbeat 3
		err = env.handleTickHeartbeat(t, d)
	case "transfer-leadership":
		// Transfer the Raft leader.
		//
		// Example:
		//
		// transfer-leadership from=1 to=4
		err = env.handleTransferLeadership(t, d)
	case "forget-leader":
		// Forgets the current leader of the given node.
		//
		// Example:
		//
		// forget-leader 1
		err = env.handleForgetLeader(t, d)
	case "send-snapshot":
		// Sends a snapshot to a node. Takes the source and destination node.
		// The message will be queued, but not delivered automatically.
		//
		// Example: send-snapshot 1 3
		env.handleSendSnapshot(t, d)
	case "propose":
		// Propose an entry.
		//
		// Example:
		//
		// propose 1 foo
		err = env.handlePropose(t, d)
	case "propose-conf-change":
		// Propose a configuration change, or transition out of a previously
		// proposed joint configuration change that requested explicit
		// transitions. When adding nodes, this command can be used to
		// logically add nodes to the configuration, but add-nodes is needed
		// to "create" the nodes.
		//
		// propose-conf-change node_id [v1=<bool>] [transition=<string>]
		// command string
		// See ConfChangesFromString for command string format.
		// Arguments are:
		//    node_id - the node proposing the configuration change.
		//    v1 - make one change at a time, false by default.
		//    transition - "auto" (the default), "explicit" or "implicit".
		// Example:
		//
		// propose-conf-change 1 transition=explicit
		// v1 v3 l4 r5
		//
		// Example:
		//
		// propose-conf-change 2 v1=true
		// v5
		err = env.handleProposeConfChange(t, d)
	case "report-unreachable":
		// Calls <1st>.ReportUnreachable(<2nd>).
		//
		// Example:
		// report-unreachable 1 2
		err = env.handleReportUnreachable(t, d)
	default:
		err = fmt.Errorf("unknown command")
	}
	// NB: the highest log level suppresses all output, including that of the
	// handlers. This comes in useful during setup which can be chatty.
	// However, errors are always logged.
	if err != nil {
		if env.Output.Quiet() {
			return err.Error()
		}
		env.Output.WriteString(err.Error())
	}
	if env.Output.Len() == 0 {
		return "ok"
	}
	return env.Output.String()
}

func firstAsInt(t *testing.T, d datadriven.TestData) int {
	return nthAsInt(t, d, 0)
}

func nthAsInt(t *testing.T, d datadriven.TestData, nth int) int {
	t.Helper()
	n, err := strconv.Atoi(d.CmdArgs[nth].Key)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func firstAsNodeIdx(t *testing.T, d datadriven.TestData) int {
	t.Helper()
	n := firstAsInt(t, d)
	return n - 1
}

func nodeIdxs(t *testing.T, d datadriven.TestData) []int {
	var ints []int
	for i := 0; i < len(d.CmdArgs); i++ {
		if len(d.CmdArgs[i].Vals) != 0 {
			continue
		}
		n, err := strconv.Atoi(d.CmdArgs[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		ints = append(ints, n-1)
	}
	return ints
}
