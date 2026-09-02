import { tableKey } from "./address.mjs";
import { FlowState } from "./flow-states.mjs";

export class MstPlanner {
  plan(flows, { localPeers = [] } = {}) {
    const groups = groupByTable(flows);
    const selected = new Set();
    const offloaded = new Set();
    const localPeerSet = new Set(localPeers);

    for (const tableFlows of groups.values()) {
      const edgeGroups = groupByUndirectedEdge(tableFlows);
      const candidates = [];

      for (const edgeFlows of edgeGroups.values()) {
        const candidate = choosePrimaryFlow(edgeFlows, localPeerSet);
        if (candidate) candidates.push(candidate);
      }

      for (const flow of computePrim(candidates)) {
        selected.add(flow.id);
      }

      for (const edgeFlows of edgeGroups.values()) {
        const primary = edgeFlows.find((flow) => selected.has(flow.id));
        if (!primary) continue;

        for (const flow of edgeFlows) {
          if (flow.id !== primary.id) offloaded.add(flow.id);
        }
      }
    }

    return {
      selectedIds: [...selected],
      offloadedIds: [...offloaded],
    };
  }
}

function groupByTable(flows) {
  const groups = new Map();
  for (const flow of flows) {
    const key = tableKey(flow.lobby_id, flow.table_id);
    const current = groups.get(key) ?? [];
    current.push(flow);
    groups.set(key, current);
  }
  return groups;
}

function groupByUndirectedEdge(flows) {
  const groups = new Map();
  for (const flow of flows) {
    const key = undirectedEdgeKey(flow);
    const current = groups.get(key) ?? [];
    current.push(flow);
    groups.set(key, current);
  }
  return groups;
}

function choosePrimaryFlow(flows, localPeers) {
  const committed = flows
    .filter((flow) => committedPrimaryStates.has(flow.mst_state))
    .sort(compareFlowPriority)[0];
  if (committed) return committed;

  return flows
    .filter((flow) => mutableInputStates.has(flow.mst_state))
    .filter((flow) => localPeers.has(flow.from_peer))
    .sort(compareFlowPriority)[0] ?? null;
}

function computePrim(flows) {
  const nodes = new Set();
  for (const flow of flows) {
    nodes.add(flow.from_peer);
    nodes.add(flow.to_peer);
  }

  const selected = [];
  const visited = new Set();
  for (const start of nodes) {
    if (visited.has(start)) continue;

    visited.add(start);
    const frontier = [];
    pushEdges(start, flows, visited, frontier);

    while (frontier.length > 0) {
      frontier.sort(compareFlowPriority);
      const edge = frontier.shift();
      const next = visited.has(edge.from_peer) ? edge.to_peer : edge.from_peer;
      if (visited.has(next)) continue;

      visited.add(next);
      selected.push(edge);
      pushEdges(next, flows, visited, frontier);
    }
  }
  return selected;
}

function pushEdges(node, flows, visited, frontier) {
  for (const flow of flows) {
    const touches = flow.from_peer === node || flow.to_peer === node;
    const crosses = !visited.has(flow.from_peer) || !visited.has(flow.to_peer);
    if (touches && crosses) frontier.push(flow);
  }
}

function compareFlowPriority(a, b) {
  const age = new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
  return age || String(b.id).localeCompare(String(a.id));
}

function undirectedEdgeKey(flow) {
  return [flow.lobby_id, flow.table_id, ...[flow.from_peer, flow.to_peer].sort()].join(":");
}

const committedPrimaryStates = new Set([
  FlowState.REQUEST_OFFER,
  FlowState.OFFER,
  FlowState.ANSWER,
]);

const mutableInputStates = new Set([
  FlowState.ANNOUNCED,
  FlowState.PROCESSING,
]);
