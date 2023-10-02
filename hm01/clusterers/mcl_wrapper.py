from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import List, Iterator, Dict, Optional, Tuple, Union
from collections import defaultdict
import csv

import networkit as nk

from clusterers.abstract_clusterer import AbstractClusterer

from graph import Graph, IntangibleSubgraph, RealizedSubgraph
from context import context


@dataclass
class MCLClusterer(AbstractClusterer):
    inflation: int

    def cluster(self, graph: Union[Graph, RealizedSubgraph]) -> Iterator[IntangibleSubgraph]:
        """Returns a list of (labeled) subgraphs on the graph"""
        cluster_id = graph.index  # the cluster id such as 5a6b2

        old_to_new_node_id_mapping = graph.continuous_ids
        new_to_old_node_id_mapping = {
            v: k for k, v in old_to_new_node_id_mapping.items()
        }
        mcl_clustering_output_filename = context.request_graph_related_path(
            graph, "mcl"
        )
        self.run_mcl(
            graph.as_compact_abc_edgelist_filepath(),
            graph,
            mcl_clustering_output_filename,
        )

        with open(mcl_clustering_output_filename, "r") as f:
            for local_cluster_id,line in enumerate(f):
                local_cluster_member_arr = line.strip().split()
                # global_cluster_id = f"{cluster_id}{local_cluster_id}"
                global_cluster_member_arr = [
                    int(new_to_old_node_id_mapping[int(local_id)])
                    for local_id in local_cluster_member_arr
                ]
                yield graph.intangible_subgraph(
                    global_cluster_member_arr, str(local_cluster_id)
                )

    def run_mcl(self, edge_list_path, graph: Union[Graph, RealizedSubgraph], output_file):
        """Runs MCL given an edge list"""
        mcl_path = context.mcl_path
        stderr_p = context.request_graph_related_path(graph, f"_mcl_inflation={self.inflation}.stderr")
        stdout_p = context.request_graph_related_path(graph, f"_mcl_inflation={self.inflation}.stdout")
        with open(stderr_p, "w") as f_err:
            with open(stdout_p, "w") as f_out:
                subprocess.run(
                    [
                        "/usr/bin/time",
                        "-v",
                        mcl_path,
                        edge_list_path,
                        "--abc",
                        "-o",
                        output_file,
                        "-I",
                        str(self.inflation),
                    ]
                )

    def from_existing_clustering(self, filepath) -> List[IntangibleSubgraph]:
        clusters: Dict[str, IntangibleSubgraph] = {}
        with open(filepath, "r") as f:
            for cluster_id,line in enumerate(f):
                cluster_member_arr = [int(node_id) for node_id in line.strip().split()]
                # global_cluster_id = f"{cluster_id}{local_cluster_id}"
                clusters[str(cluster_id)] = IntangibleSubgraph(cluster_member_arr, str(cluster_id))
        return list(v for v in clusters.values() if v.n() > 1)
