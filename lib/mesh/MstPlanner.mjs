import { tableKey } from "./address.mjs";

export class MstPlanner {
  plan(flows) {
    const groups = groupByTable(flows);
    const selected = new Set();

    for (const tableFlows of groups.values()) {
      for (const flow of computePrim(tableFlows)) {
        selected.add(flow.id);
      }
    }

    return {
      selectedIds: [...selected],
      offloadedIds: flows
        .filter((flow) => !selected.has(flow.id))
        .map((flow) => flow.id),
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
      frontier.sort(compareFlowAge);
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

function compareFlowAge(a, b) {
  return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
}
