// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import _ from "lodash";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { AxisUnits } from "@cockroachlabs/cluster-ui";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const {
    nodeIDs,
    nodeSources,
    tooltipSelection,
    nodeDisplayNameByID,
    tenantSource,
  } = props;

  return [
    <LineGraph
      title="Live Node Count"
      tenantSource={tenantSource}
      tooltip={`The number of live nodes in the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="nodes">
        <Metric
          name="cr.node.liveness.livenodes"
          title="Live Nodes"
          aggregateMax
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Memory Usage"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={
        <div>
          {`Memory in use ${tooltipSelection}:`}
          <dl>
            <dt>RSS</dt>
            <dd>Total memory in use by CockroachDB</dd>
            <dt>Go Allocated</dt>
            <dd>Memory allocated by the Go layer</dd>
            <dt>Go Total</dt>
            <dd>Total memory managed by the Go layer</dd>
            <dt>C Allocated</dt>
            <dd>Memory allocated by the C layer</dd>
            <dt>C Total</dt>
            <dd>Total memory managed by the C layer</dd>
          </dl>
        </div>
      }
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Bytes} label="memory usage">
        <Metric name="cr.node.sys.rss" title="Total memory (RSS)" />
        <Metric name="cr.node.sys.go.allocbytes" title="Go Allocated" />
        <Metric name="cr.node.sys.go.totalbytes" title="Go Total" />
        <Metric name="cr.node.sys.cgo.allocbytes" title="CGo Allocated" />
        <Metric name="cr.node.sys.cgo.totalbytes" title="CGo Total" />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Goroutine Count"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines ${tooltipSelection}. This count should rise
          and fall based on load.`}
      showMetricsInTooltip={true}
    >
      <Axis label="goroutines">
        {nodeIDs.map(nid => (
            <Metric
                name="cr.node.sys.goroutines"
                title={nodeDisplayName(nodeDisplayNameByID, nid)}
                sources={[nid]}
            />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Runnable Goroutines per CPU"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of Goroutines waiting for CPU ${tooltipSelection}. This
          count should rise and fall based on load.`}
      showMetricsInTooltip={true}
    >
      <Axis label="goroutines">
        {nodeIDs.map(nid => (
          <Metric
            name="cr.node.sys.runnable.goroutines.per.cpu"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    // TODO(mrtracy): The following two graphs are a good first example of a graph with
    // two axes; the two series should be highly correlated, but have different units.
    <LineGraph
      title="GC Runs"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The number of times that Go’s garbage collector was invoked per second ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis label="runs">
        {nodeIDs.map(nid => (
            <Metric
                name="cr.node.sys.gc.count"
                title={nodeDisplayName(nodeDisplayNameByID, nid)}
                sources={[nid]}
                nonNegativeRate
            />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="GC Stop-the-World Pause Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The amount of time Go’s garbage collector spent in stop-the-world pauses per second
          ${tooltipSelection}. During stop-the-world garbage collection phases (sweep termination
          and mark termination), application code execution is paused.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="STW pause time">
        {nodeIDs.map(nid => (
            <Metric
                name="cr.node.sys.gc.pause.ns"
                title={nodeDisplayName(nodeDisplayNameByID, nid)}
                sources={[nid]}
                nonNegativeRate
            />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="CPU Time"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`The amount of CPU time used by CockroachDB (User) and system-level
          operations (Sys) ${tooltipSelection}.`}
      showMetricsInTooltip={true}
    >
      <Axis units={AxisUnits.Duration} label="cpu time">
        <Metric
          name="cr.node.sys.cpu.user.ns"
          title="User CPU Time"
          nonNegativeRate
        />
        <Metric
          name="cr.node.sys.cpu.sys.ns"
          title="Sys CPU Time"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Clock Offset"
      sources={nodeSources}
      tenantSource={tenantSource}
      tooltip={`Mean clock offset of each node against the rest of the cluster.`}
      showMetricsInTooltip={true}
    >
      <Axis label="offset" units={AxisUnits.Duration}>
        {_.map(nodeIDs, nid => (
          <Metric
            key={nid}
            name="cr.node.clock-offset.meannanos"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,
  ];
}
