from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import List, Iterator, Dict, Optional, Tuple, Union
from collections import defaultdict
import csv

from infomap import Infomap
import networkit as nk

from clusterers.abstract_clusterer import AbstractClusterer

from graph import Graph, IntangibleSubgraph, RealizedSubgraph
from context import context


@dataclass
class InfomapClusterer(AbstractClusterer):

    def cluster(self, graph: Union[Graph, RealizedSubgraph]) -> Iterator[IntangibleSubgraph]:
        """Returns a list of (labeled) subgraphs on the graph"""
        im = Infomap() # add options such as the level or the directed ness
        for u in graph.nodes():
            for v in graph.neighbors(u):
                im.add_link(u, v)
        im.run()
        cluster_dict = {}
        for node in im.tree:
            if node.is_leaf:
                if(node.module_id not in cluster_dict):
                    cluster_dict[node.module_id] = []
                cluster_dict[node.module_id].append(node.node_id)

        for local_cluster_id,cluster_member_arr in cluster_dict.items():
            yield graph.intangible_subgraph(
                cluster_member_arr, str(local_cluster_id)
            )

    def from_existing_clustering(self, filepath) -> List[IntangibleSubgraph]:
        # for clu file
        clusters: Dict[str, IntangibleSubgraph] = {}
        with open(filepath) as f:
            for line in f:
                node_id,cluster_id,_ = line.split()
                clusters.setdefault(
                    cluster_id, IntangibleSubgraph([], cluster_id)
                ).subset.append(int(node_id))
        return list(v for v in clusters.values() if v.n() > 1)
