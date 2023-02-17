"""The main CLI logic, containing also the main algorithm"""
from __future__ import annotations

from typing import List, Optional, Tuple, Union, Dict, Deque, cast
from dataclasses import dataclass
from collections import deque
from enum import Enum
from itertools import chain
from structlog import get_logger

import typer
import math
import time
import treeswift as ts
import networkit as nk
import jsonpickle

from hm01.clusterers.abstract_clusterer import AbstractClusterer
from hm01.clusterers.ikc_wrapper import IkcClusterer
from hm01.clusterers.leiden_wrapper import LeidenClusterer, Quality
from hm01.context import context
from hm01.mincut_requirement import MincutRequirement
from hm01.graph import Graph, IntangibleSubgraph, RealizedSubgraph
from hm01.pruner import prune_graph

import sys
import sqlite3
import pickle as pkl

class ClustererSpec(str, Enum):
    """ Container for Clusterer Specification """  
    leiden = "leiden"
    ikc = "ikc"
    leiden_mod = "leiden_mod"

class ClusterTreeNode(ts.Node):
    """ Object to represent a cluster in the mincut/recluster recursion tree 
    
    The root of the tree is the entire graph. When a a cluster is cut and reclustered into new clusters,
    the original cluster is a parent node to the children clusters.
    """
    extant: bool
    graph_index: str
    num_nodes: int
    cut_size: Optional[int]
    validity_threshold: Optional[float]

def annotate_tree_node(
    node: ClusterTreeNode, graph: Union[Graph, IntangibleSubgraph, RealizedSubgraph]
):
    """ Labels a ClusterTreeNode with its respective cluster """
    node.label = graph.index
    node.graph_index = graph.index
    node.num_nodes = graph.n()
    node.extant = False

def update_cid_membership(
    subgraph: Union[Graph, IntangibleSubgraph, RealizedSubgraph],
    node2cids: Dict[int, str],
):
    """ Set nodes within current cluster to its respective cluster id """
    for n in subgraph.nodes():
        node2cids[n] = subgraph.index

def summarize_graphs(graphs: List[IntangibleSubgraph]) -> str:
    """ Summarize graphs for logging purposes """
    if not graphs:
        return "[](empty)"
    if len(graphs) > 3:
        return f"[{graphs[0].index}, ..., {graphs[-1].index}]({len(graphs)})"
    else:
        return f"[{', '.join([g.index for g in graphs])}]({len(graphs)})"

def algorithm_g(
    global_graph: Graph,
    graphs: List[IntangibleSubgraph],
    clusterer: Union[IkcClusterer, LeidenClusterer],
    requirement: MincutRequirement,
    quiet: bool
) -> Tuple[List[IntangibleSubgraph], Dict[int, str], ts.Tree]:
    """ Main algorithm in hm01 
    
    Params:
        global_graph (Graph)                                : full graph from input
        graph (List[IntangibleSubgraph])                    : list of clusters
        clusterer (Union[IkcClusterer, LeidenClusterer])    : clustering algorithm
        requirement (MincutRequirement)                     : mincut connectivity requirement
    """
    tree = ts.Tree()                                # tree: Recursion tree that keeps track of clusters created by serial mincut/reclusters
    tree.root = ClusterTreeNode()
    annotate_tree_node(tree.root, global_graph)
    node_mapping: Dict[str, ClusterTreeNode] = {}   # node_mapping: maps cluster id to cluster tree node       
    for g in graphs:
        n = ClusterTreeNode()
        annotate_tree_node(n, g)
        tree.root.add_child(n)
        node_mapping[g.index] = n
    stack: List[IntangibleSubgraph] = list(graphs)  # stack: (TODO: Change to queue), the stack for cluster processing
    ans: List[IntangibleSubgraph] = []              # ans: Reclustered output
    node2cids: Dict[int, str] = {}                  # node2cids: Mapping between nodes and cluster ID
    
    if not quiet:
        log = get_logger()
        log.info("starting algorithm-g", queue_size=len(stack))

    # Main algorithm loop: Recursively cut clusters in stack until they have mincut above threshold
    while stack:
        if not quiet:
            log = get_logger()
            log.debug("entered next iteration of loop", queue_size=len(stack))
        
        intangible_subgraph = stack.pop()

        if not quiet:
            log.debug(
                "popped graph",
                graph_n=intangible_subgraph.n(),
                graph_index=intangible_subgraph.index,
            )

        # Mark nodes in popped cluster with their respective cluster ID
        update_cid_membership(intangible_subgraph, node2cids)

        # If the current cluster is a singleton or empty, move on
        if intangible_subgraph.n() <= 1:
            continue
        
        # Realize the set of nodes contained by the graph (i.e. construct its adjacency list)
        subgraph = intangible_subgraph.realize(global_graph)

        # Get the current cluster tree node
        tree_node = node_mapping[subgraph.index]
        if not quiet:
            log = log.bind(
                g_id=subgraph.index,
                g_n=subgraph.n(),
                g_m=subgraph.m(),
                g_mcd=subgraph.mcd(),
            )
        
        # Get minimum node degree in current cluster
        original_mcd = subgraph.mcd()

        # Pruning: Remove singletons with node degree under threshold until there exists none
        num_pruned = prune_graph(subgraph, requirement, clusterer)
        if num_pruned > 0:
            # Set the cluster cut size to the degree of the removed node
            tree_node.cut_size = original_mcd
            if not quiet:
                log = log.bind(
                    g_id=subgraph.index,
                    g_n=subgraph.n(),
                    g_m=subgraph.m(),
                    g_mcd=subgraph.mcd(),
                )
                log.info("pruned graph", num_pruned=num_pruned)

            # Create a TreeNodeCluster for the pruned cluster and set it as the current node's child
            new_child = ClusterTreeNode()
            subgraph.index = f"{subgraph.index}δ"
            annotate_tree_node(new_child, subgraph)
            tree_node.add_child(new_child)
            node_mapping[subgraph.index] = new_child

            # Iterate to the new node
            tree_node = new_child
            update_cid_membership(subgraph, node2cids)
        
        # Compute the mincut of the cluster
        mincut_res = subgraph.find_mincut()

        # is a cluster "cut-valid" -- having good connectivity?
        valid_threshold = requirement.validity_threshold(clusterer, subgraph)
        if not quiet:
            log.debug("calculated validity threshold", validity_threshold=valid_threshold)
            log.debug(
                "mincut computed",
                a_side_size=len(mincut_res.light_partition),
                b_side_size=len(mincut_res.heavy_partition),
                cut_size=mincut_res.cut_size,
            )
        
        # Set the current cluster's cut size
        tree_node.cut_size = mincut_res.cut_size
        tree_node.validity_threshold = valid_threshold

        # If the cut size is below validity, split!
        if mincut_res.cut_size <= valid_threshold and mincut_res.cut_size > 0:
            # Split partitions and set them as children nodes
            p1, p2 = subgraph.cut_by_mincut(mincut_res)
            node_a = ClusterTreeNode()
            node_b = ClusterTreeNode()
            annotate_tree_node(node_a, p1)
            annotate_tree_node(node_b, p2)
            tree_node.add_child(node_a)
            tree_node.add_child(node_b)
            node_mapping[p1.index] = node_a
            node_mapping[p2.index] = node_b

            # Cluster both partitions
            subp1 = list(clusterer.cluster_without_singletons(p1))
            subp2 = list(clusterer.cluster_without_singletons(p2))

            # Set clusters as children of the partitions
            for p, np in [(subp1, node_a), (subp2, node_b)]:
                for sg in p:
                    n = ClusterTreeNode()
                    annotate_tree_node(n, sg)
                    node_mapping[sg.index] = n
                    np.add_child(n)

            # Add the new clusters to the stack
            stack.extend(subp1)
            stack.extend(subp2)

            if not quiet:
                log.info(
                    "cluster split",
                    num_a_side=len(subp1),
                    num_b_side=len(subp2),
                    summary_a_side=summarize_graphs(subp1),
                    summary_b_side=summarize_graphs(subp2),
                )
        else:
            # Compute the modularity of the cluster
            candidate = subgraph.to_intangible(global_graph)
            mod = global_graph.modularity_of(candidate)

            # Check if the modularity value is valid so that the answer can include the modified cluster
            if not isinstance(clusterer, IkcClusterer) or mod > 0:
                ans.append(candidate)
                node_mapping[subgraph.index].extant = True
                if not quiet:
                    log.info("cut valid, not splitting anymore")
            else:
                node_mapping[subgraph.index].extant = False
                if not quiet:
                    log.info(
                        "cut valid, but modularity non-positive, thrown away",
                        modularity=mod,
                    )
    return ans, node2cids, tree

def main(
    input: str = typer.Option(..., "--input", "-i"),
    existing_clustering: str = typer.Option(..., "--existing-clustering", "-e"),
    quiet: Optional[bool] = typer.Option(False, "--quiet", "-q"),
    working_dir: Optional[str] = typer.Option("", "--working-dir", "-d"),
    clusterer_spec: ClustererSpec = typer.Option(..., "--clusterer", "-c"),
    k: int = typer.Option(-1, "--k", "-k"),
    resolution: float = typer.Option(-1, "--resolution", "-g"),
    threshold: str = typer.Option("", "--threshold", "-t"),
    output: str = typer.Option("", "--output", "-o")
):
    """ Connectivity-Modifier (CM). Take a network and cluster it ensuring cut validity

    Parameters:
        input (str)                     : filename of input graph
        existing_clustering (str)       : filename of existing clustering
        quiet (bool)                    : silence output messages
        working_dir (str)               : name of temporary directory to store mid-stage data (optional)
        clusterer_spec (ClusterSpec)    : clusterering algorithm
        k (int)                         : k param (for IKC only)
        resolution (float)              : resolution param (for Leiden only)
        threshold (str)                 : connectivity requiremen, can be in terms of log(N)
        output (str)                    : filename to store output
    """
    # Setting a really high recursion limit to prevent stack overflow errors
    sys.setrecursionlimit(1231231234)

    # Check -g and -k parameters for Leiden and IKC respectively
    if clusterer_spec == ClustererSpec.leiden:
        assert resolution != -1, "Leiden requires resolution"
        clusterer: Union[LeidenClusterer, IkcClusterer] = LeidenClusterer(resolution)
    elif clusterer_spec == ClustererSpec.leiden_mod:
        assert resolution == -1, "Leiden with modularity does not support resolution"
        clusterer = LeidenClusterer(resolution, quality=Quality.modularity)
    else:
        assert k != -1, "IKC requires k"
        clusterer = IkcClusterer(k)

    # Start hm01
    if not quiet:
        log = get_logger()
        context.with_working_dir(input + "_working_dir" if not working_dir else working_dir)
        log.info(
            f"starting hm01",
            input=input,
            working_dir=context.working_dir,
            clusterer=clusterer,
        )

    # Parse mincut threshold specification
    requirement = MincutRequirement.try_from_str(threshold)
    if not quiet:
        log.info(f"parsed connectivity requirement", requirement=requirement)

    time1 = time.time()

    # Load full graph into Graph object
    edgelist_reader = nk.graphio.EdgeListReader("\t", 0)
    nk_graph = edgelist_reader.read(input)
    if not quiet:
        log.info(
            f"loaded graph",
            n=nk_graph.numberOfNodes(),
            m=nk_graph.numberOfEdges(),
            elapsed=time.time() - time1,
        )
    root_graph = Graph(nk_graph, "")

    # Load clustering
    if not quiet:
        log.info(f"loading existing clustering before algorithm-g", clusterer=clusterer)
    clusters = clusterer.from_existing_clustering(existing_clustering)
    if not quiet:
        log.info(
            f"first round of clustering obtained",
            num_clusters=len(clusters),
            summary=summarize_graphs(clusters),
        )

    # Call the main CM algorithm
    new_clusters, labels, tree = algorithm_g(
        root_graph, clusters, clusterer, requirement, quiet
    )

    # Retrieve output
    with open(output, "w+") as f:
        for n, cid in labels.items():
            f.write(f"{n} {cid}\n")
    with open(output + ".tree.json", "w+") as f:
        f.write(cast(str, jsonpickle.encode(tree)))

def entry_point():
    typer.run(main)


if __name__ == "__main__":
    entry_point()
