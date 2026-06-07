import asyncio
import logging
import random
import time
from typing import Dict, Any, List, Set, Tuple

try:
    from neo4j import GraphDatabase, AsyncGraphDatabase
    HAS_NEO4J_SDK = True
except ImportError:
    HAS_NEO4J_SDK = False

logger = logging.getLogger("TopologyConnector")

class MockNeo4jClient:
    def __init__(self):
        self._graph: Dict[str, Dict[str, Any]] = {}
        logger.info("Fallback: InMemory MockNeo4jClient initialized.")

    async def add_node(self, node_id: str, label: str, properties: Dict[str, Any]):
        await asyncio.sleep(0.001)
        if node_id not in self._graph:
            self._graph[node_id] = {"label": label, "properties": properties, "edges": {}}
        else:
            self._graph[node_id]["properties"].update(properties)

    async def add_edge(self, source_id: str, target_id: str, rel_type: str, weight: float = 1.0):
        await asyncio.sleep(0.001)
        if source_id in self._graph and target_id in self._graph:
            self._graph[source_id]["edges"][target_id] = {"rel_type": rel_type, "weight": weight}

    async def get_subgraph(self, start_node_id: str, depth: int = 2) -> Dict[str, Any]:
        await asyncio.sleep(0.005)
        if start_node_id not in self._graph:
            return {"nodes": {}, "edges": []}
        nodes_in_subgraph = {}
        edges_in_subgraph = []
        queue = [(start_node_id, 0)]
        visited = {start_node_id}
        while queue:
            curr_id, curr_depth = queue.pop(0)
            node_info = self._graph[curr_id]
            nodes_in_subgraph[curr_id] = {
                "label": node_info["label"],
                "properties": node_info["properties"],
                "depth": curr_depth
            }
            if curr_depth < depth:
                for target_id, edge_info in node_info["edges"].items():
                    edges_in_subgraph.append((curr_id, target_id, edge_info))
                    if target_id not in visited:
                        visited.add(target_id)
                        queue.append((target_id, curr_depth + 1))
        return {"nodes": nodes_in_subgraph, "edges": edges_in_subgraph}


class PhysicalNeo4jClient:
    def __init__(self, uri: str, auth: Tuple[str, str]):
        self.uri = uri
        self.auth = auth
        self.driver = None
        logger.info("PhysicalNeo4jClient driver configuration armed.")

    async def connect(self) -> bool:
        try:
            self.driver = AsyncGraphDatabase.driver(self.uri, auth=self.auth)
            await self.driver.verify_connectivity()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j instance: {e}")
            if self.driver:
                await self.driver.close()
            return False

    async def close(self):
        if self.driver:
            await self.driver.close()

    async def add_node(self, node_id: str, label: str, properties: Dict[str, Any]):
        async with self.driver.session() as session:
            query = f"MERGE (n:{label} {{id: $node_id}}) SET n += $properties"
            await session.run(query, node_id=node_id, properties=properties)

    async def add_edge(self, source_id: str, target_id: str, rel_type: str, weight: float = 1.0):
        async with self.driver.session() as session:
            query = f"MATCH (a) WHERE a.id = $source_id MATCH (b) WHERE b.id = $target_id MERGE (a)-[r:{rel_type}]->(b) SET r.weight = $weight"
            await session.run(query, source_id=source_id, target_id=target_id, weight=weight)

    async def get_subgraph(self, start_node_id: str, depth: int = 2) -> Dict[str, Any]:
        async with self.driver.session() as session:
            query = "MATCH path = (start {id: $start_node_id})-[r*0..2]->(node) RETURN path"
            result = await session.run(query, start_node_id=start_node_id)
            nodes_in_subgraph = {}
            edges_in_subgraph = []
            async for record in result:
                path = record["path"]
                for node in path.nodes:
                    node_id = node.get("id")
                    labels = list(node.labels)
                    label = labels[0] if labels else "Unknown"
                    nodes_in_subgraph[node_id] = {
                        "label": label,
                        "properties": dict(node),
                        "depth": 0 if node_id == start_node_id else 1
                    }
                for rel in path.relationships:
                    start_id = rel.nodes[0].get("id")
                    end_id = rel.nodes[1].get("id")
                    edges_in_subgraph.append((start_id, end_id, {"rel_type": rel.type, "weight": rel.get("weight", 1.0)}))
            return {"nodes": nodes_in_subgraph, "edges": edges_in_subgraph}


class ManifoldSemanticEngine:
    def __init__(self):
        self.mock_vocabulary = ["bottleneck", "cpu", "channel", "latency", "network", "performance", "normal"]
        # 缓存字典提升高频计算性能
        self._vector_cache = {}

    def _get_bag_of_words_vector(self, text: str) -> List[float]:
        # 命中缓存
        if text in self._vector_cache:
            return self._vector_cache[text]
            
        text_lower = text.lower()
        vector = []
        for word in self.mock_vocabulary:
            vector.append(float(text_lower.count(word)))
        norm = sum(x**2 for x in vector)**0.5
        if norm > 0:
            vector = [x / norm for x in vector]
            
        # 写入缓存
        self._vector_cache[text] = vector
        return vector

    def compute_cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        dot_product = sum(x * y for x, y in zip(vec1, vec2))
        norm1 = sum(x**2 for x in vec1)**0.5
        norm2 = sum(x**2 for x in vec2)**0.5
        if norm1 * norm2 == 0:
            return 0.0
        return dot_product / (norm1 * norm2)

    def calculate_manifold_score(self, topo_depth: int, semantic_sim: float) -> float:
        alpha = 0.4
        topo_score = 1.0 / (1.0 + topo_depth)
        return round(alpha * topo_score + (1.0 - alpha) * semantic_sim, 4)


class TopologyConnector:
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self.semantic_engine = ManifoldSemanticEngine()
        self.use_physical_neo4j = False
        self.neo4j = None
        if HAS_NEO4J_SDK and uri and user and password:
            client = PhysicalNeo4jClient(uri, (user, password))
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._try_init_physical(client))
            else:
                loop.run_until_complete(self._try_init_physical(client))
        if not self.use_physical_neo4j:
            self.neo4j = MockNeo4jClient()

    async def _try_init_physical(self, client: PhysicalNeo4jClient):
        success = await client.connect()
        if success:
            self.neo4j = client
            self.use_physical_neo4j = True
        else:
            self.neo4j = MockNeo4jClient()

    async def ingest_aligned_frame(self, key: str, aligned_frame: Dict[int, Any]) -> bool:
        try:
            main_node_id = f"frame_{key}"
            await self.neo4j.add_node(node_id=main_node_id, label="AlignedFrame", properties={"key": key, "ingest_time": time.time()})
            for dim_id, packet in aligned_frame.items():
                dim_node_id = f"dim_{dim_id}_{key}"
                await self.neo4j.add_node(node_id=dim_node_id, label="DimensionNode", properties={"dimension_id": dim_id, "payload": str(packet.payload), "packet_timestamp": packet.timestamp})
                await self.neo4j.add_edge(source_id=main_node_id, target_id=dim_node_id, rel_type="CONTAINS_DIMENSION")
            return True
        except Exception as e:
            logger.error(f"Failed to ingest: {e}")
            return False

    async def retrieve_topology_manifold(self, start_node_id: str, query: str, depth: int = 2) -> Dict[str, Any]:
        subgraph = await self.neo4j.get_subgraph(start_node_id, depth=depth)
        nodes = subgraph.get("nodes", {})
        edges = subgraph.get("edges", [])
        if not nodes:
            return {"query": query, "message": "Zero records found."}

        query_vector = self.semantic_engine._get_bag_of_words_vector(query)
        ranked_nodes = []
        for node_id, node_info in nodes.items():
            props = node_info.get("properties", {})
            payload_text = props.get("payload", "") or props.get("key", "")
            node_vector = self.semantic_engine._get_bag_of_words_vector(payload_text)
            semantic_sim = self.semantic_engine.compute_cosine_similarity(query_vector, node_vector)
            node_depth = node_info.get("depth", 1)
            manifold_score = self.semantic_engine.calculate_manifold_score(node_depth, semantic_sim)
            ranked_nodes.append({
                "node_id": node_id,
                "label": node_info["label"],
                "depth": node_depth,
                "semantic_similarity": semantic_sim,
                "manifold_score": manifold_score
            })
        ranked_nodes.sort(key=lambda x: x["manifold_score"], reverse=True)

        # 动态计算图拓扑指标：图谱密度与平均度数
        n_count = len(nodes)
        e_count = len(edges)
        graph_density = round(2 * e_count / (n_count * (n_count - 1)), 4) if n_count > 1 else 0.0
        avg_degree = round(e_count / n_count, 2) if n_count > 0 else 0.0

        return {
            "query": query,
            "anchor_node": start_node_id,
            "engine_mode": "PHYSICAL_NEO4J" if self.use_physical_neo4j else "IN_MEMORY_FALLBACK",
            "topology_subgraph": {
                "nodes_count": n_count,
                "edges_count": e_count,
                "graph_density": graph_density,
                "average_degree": avg_degree,
                "all_nodes": list(nodes.keys())
            },
            "manifold_retrieval_ranked": ranked_nodes[:5]
        }

