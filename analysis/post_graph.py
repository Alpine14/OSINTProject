"""
Post graph module for OSINT analysis.

Builds an undirected graph where posts are nodes and edges connect
posts that share at least a configurable number of keywords.
Uses only stdlib (collections.deque, dataclasses).
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional

from models.post import OSINTPost

logger = logging.getLogger(__name__)


@dataclass
class Edge:
    """An undirected edge between two posts sharing keywords."""
    post_id_a: str
    post_id_b: str
    shared_keywords: Set[str]

    @property
    def weight(self) -> int:
        return len(self.shared_keywords)


class PostGraph:
    """
    Graph where posts are nodes and edges connect posts sharing keywords.

    Attributes:
        min_shared_keywords: Minimum shared keywords to create an edge
    """

    def __init__(self, min_shared_keywords: int = 2):
        self.min_shared_keywords = min_shared_keywords
        self._nodes: Dict[str, OSINTPost] = {}
        self._adj: Dict[str, Dict[str, Edge]] = {}
        self._keyword_index: Dict[str, Set[str]] = {}  # keyword -> set of post_ids

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    def add_post(self, post: OSINTPost) -> int:
        """
        Add a post as a node and create edges to related posts.

        Args:
            post: The post to add

        Returns:
            Number of edges created
        """
        if post.post_id in self._nodes:
            return 0

        self._nodes[post.post_id] = post
        self._adj.setdefault(post.post_id, {})

        post_keywords = set(kw.lower() for kw in post.matched_keywords)

        # Find candidate posts via the inverted keyword index
        candidate_counts: Dict[str, Set[str]] = {}
        for kw in post_keywords:
            for other_id in self._keyword_index.get(kw, set()):
                candidate_counts.setdefault(other_id, set()).add(kw)

        edges_created = 0
        for other_id, shared in candidate_counts.items():
            if len(shared) >= self.min_shared_keywords:
                edge = Edge(
                    post_id_a=post.post_id,
                    post_id_b=other_id,
                    shared_keywords=shared,
                )
                self._adj[post.post_id][other_id] = edge
                self._adj.setdefault(other_id, {})[post.post_id] = edge
                edges_created += 1

        # Update the inverted index
        for kw in post_keywords:
            self._keyword_index.setdefault(kw, set()).add(post.post_id)

        return edges_created

    def add_posts(self, posts: List[OSINTPost]) -> Dict:
        """
        Batch-add posts.

        Returns:
            Stats dict with nodes_added and edges_created
        """
        nodes_added = 0
        edges_created = 0
        for post in posts:
            if post.post_id not in self._nodes:
                edges_created += self.add_post(post)
                nodes_added += 1
        return {"nodes_added": nodes_added, "edges_created": edges_created}

    def has_post(self, post_id: str) -> bool:
        return post_id in self._nodes

    def get_post(self, post_id: str) -> Optional[OSINTPost]:
        return self._nodes.get(post_id)

    def remove_post(self, post_id: str) -> None:
        """Remove a post node and all its edges."""
        if post_id not in self._nodes:
            return

        post = self._nodes[post_id]
        post_keywords = set(kw.lower() for kw in post.matched_keywords)

        # Remove from keyword index
        for kw in post_keywords:
            if kw in self._keyword_index:
                self._keyword_index[kw].discard(post_id)
                if not self._keyword_index[kw]:
                    del self._keyword_index[kw]

        # Remove edges from neighbors
        for neighbor_id in list(self._adj.get(post_id, {})):
            self._adj[neighbor_id].pop(post_id, None)

        del self._adj[post_id]
        del self._nodes[post_id]

    # ------------------------------------------------------------------
    # Edge / neighbor operations
    # ------------------------------------------------------------------

    def get_neighbors(self, post_id: str) -> List[Tuple[OSINTPost, Edge]]:
        """Get all neighbors of a post with their edges."""
        result = []
        for neighbor_id, edge in self._adj.get(post_id, {}).items():
            neighbor = self._nodes.get(neighbor_id)
            if neighbor:
                result.append((neighbor, edge))
        return result

    def get_shared_keywords(self, id_a: str, id_b: str) -> Set[str]:
        """Get the shared keywords between two posts, or empty set if no edge."""
        edge = self._adj.get(id_a, {}).get(id_b)
        if edge:
            return set(edge.shared_keywords)
        return set()

    # ------------------------------------------------------------------
    # Graph analysis
    # ------------------------------------------------------------------

    def get_connected_components(self) -> List[List[str]]:
        """
        Find connected components using BFS.

        Returns:
            List of components (each a list of post_ids), largest first
        """
        visited: Set[str] = set()
        components: List[List[str]] = []

        for node_id in self._nodes:
            if node_id in visited:
                continue
            component: List[str] = []
            queue = deque([node_id])
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                component.append(current)
                for neighbor_id in self._adj.get(current, {}):
                    if neighbor_id not in visited:
                        queue.append(neighbor_id)
            components.append(component)

        components.sort(key=len, reverse=True)
        return components

    def get_clusters(self, min_size: int = 2) -> List[List[OSINTPost]]:
        """
        Get connected components with at least min_size posts.

        Returns:
            List of clusters (each a list of OSINTPost), largest first
        """
        components = self.get_connected_components()
        clusters = []
        for comp in components:
            if len(comp) >= min_size:
                clusters.append([self._nodes[pid] for pid in comp])
        return clusters

    def get_cluster_keywords(self, cluster_ids: List[str]) -> Dict[str, int]:
        """
        Get keyword frequency within a cluster.

        Args:
            cluster_ids: List of post_ids in the cluster

        Returns:
            Dict mapping keyword -> count
        """
        counts: Dict[str, int] = {}
        for pid in cluster_ids:
            post = self._nodes.get(pid)
            if post:
                for kw in post.matched_keywords:
                    kw_lower = kw.lower()
                    counts[kw_lower] = counts.get(kw_lower, 0) + 1
        return counts

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        total = sum(len(neighbors) for neighbors in self._adj.values())
        return total // 2  # each edge stored in both directions

    def get_stats(self) -> Dict:
        """Return summary statistics about the graph."""
        components = self.get_connected_components()
        degrees = [len(self._adj.get(pid, {})) for pid in self._nodes]
        avg_degree = sum(degrees) / max(1, len(degrees))
        return {
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "component_count": len(components),
            "largest_component_size": len(components[0]) if components else 0,
            "avg_degree": round(avg_degree, 2),
        }

    def get_most_connected(self, limit: int = 10) -> List[Tuple[OSINTPost, int]]:
        """
        Get the most connected posts by degree.

        Returns:
            List of (post, degree) tuples sorted by degree descending
        """
        items = [
            (self._nodes[pid], len(neighbors))
            for pid, neighbors in self._adj.items()
            if pid in self._nodes
        ]
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:limit]

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_posts(cls, posts: List[OSINTPost], min_shared: int = 2) -> "PostGraph":
        """Build a graph from a list of posts."""
        graph = cls(min_shared_keywords=min_shared)
        graph.add_posts(posts)
        return graph

    @classmethod
    def from_storage(cls, storage, min_shared: int = 2, **kwargs) -> "PostGraph":
        """
        Build a graph from all posts in storage.

        Args:
            storage: Storage instance with get_all_posts()
            min_shared: Minimum shared keywords for an edge
            **kwargs: Passed to storage.get_all_posts()
        """
        posts = storage.get_all_posts(**kwargs)
        return cls.from_posts(posts, min_shared=min_shared)
