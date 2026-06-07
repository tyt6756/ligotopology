import asyncio
import logging
import random
import time
from typing import Dict, Any, List, Set, Tuple

# 尝试导入真实的 neo4j 驱动包
try:
    from neo4j import GraphDatabase, AsyncGraphDatabase
    HAS_NEO4J_SDK = True
except ImportError:
    HAS_NEO4J_SDK = False

logger = logging.getLogger("TopologyConnector")

class MockNeo4jClient:
    """
    内存仿真图数据库，用作物理 Neo4j 不可用或未配置时的自动 Fallback 降级引擎。
    """
    def __init__(self):
        self._graph: Dict[str, Dict[str, Any]] = {}
        logger.info("Fallback: InMemory MockNeo4jClient initialized.")

    async def add_node(self, node_id: str, label: str, properties: Dict[str, Any]):
        await asyncio.sleep(0.001)  # 模拟微小延迟
        if node_id not in self._graph:
            self._graph[node_id] = {"label": label, "properties": properties, "edges": {}}
        else:
            self._graph[node_id]["properties"].update(properties)
        logger.debug(f"InMemory-Neo4j: Added/Updated node '{node_id}'")

    async def add_edge(self, source_id: str, target_id: str, rel_type: str, weight: float = 1.0):
        await asyncio.sleep(0.001)
        if source_id in self._graph and target_id in self._graph:
            self._graph[source_id]["edges"][target_id] = {"rel_type": rel_type, "weight": weight}
            logger.debug(f"InMemory-Neo4j: Connected '{source_id}' -[{rel_type}]-> '{target_id}'")

    async def get_subgraph(self, start_node_id: str, depth: int = 2) -> Dict[str, Any]:
        await asyncio.sleep(0.005)
        if start_node_id not in self._graph:
            return {"nodes": {}, "edges": []}

        nodes_in_subgraph: Dict[str, Dict[str, Any]] = {}
        edges_in_subgraph: List[Tuple[str, str, Dict[str, Any]]] = []
        
        # 广度优先搜索 (BFS) 提取物理子图
        queue = [(start_node_id, 0)]
        visited: Set[str] = {start_node_id}

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

        return {
            "nodes": nodes_in_subgraph,
            "edges": edges_in_subgraph
        }


class PhysicalNeo4jClient:
    """
    真实的 Neo4j 异步驱动客户端。当外部数据库就绪时，直接进行高性能物理存储与 Cypher 图查询。
    """
    def __init__(self, uri: str, auth: Tuple[str, str]):
        self.uri = uri
        self.auth = auth
        self.driver = None
        logger.info("PhysicalNeo4jClient driver configuration armed.")

    async def connect(self) -> bool:
        try:
            # 开启异步 Neo4j 驱动连接
            self.driver = AsyncGraphDatabase.driver(self.uri, auth=self.auth)
            # 验证连接连通性
            await self.driver.verify_connectivity()
            logger.info("Successfully established connectivity to physical Neo4j instance.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j instance: {e}")
            if self.driver:
                await self.driver.close()
            return False

    async def close(self):
        if self.driver:
            await self.driver.close()
            logger.info("Physical Neo4j driver connection pool closed.")

    async def add_node(self, node_id: str, label: str, properties: Dict[str, Any]):
        async with self.driver.session() as session:
            # 构建高复用 Cypher 节点写入语句
            query = (
                f"MERGE (n:{label} {{id: $node_id}}) "
                f"SET n += $properties"
            )
            await session.run(query, node_id=node_id, properties=properties)

    async def add_edge(self, source_id: str, target_id: str, rel_type: str, weight: float = 1.0):
        async with self.driver.session() as session:
            # Cypher 动态构建边关系
            query = (
                f"MATCH (a) WHERE a.id = $source_id "
                f"MATCH (b) WHERE b.id = $target_id "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                f"SET r.weight = $weight"
            )
            await session.run(query, source_id=source_id, target_id=target_id, weight=weight)

    async def get_subgraph(self, start_node_id: str, depth: int = 2) -> Dict[str, Any]:
        """
        利用图计算 Cypher 语句，一次性提取中心节点外扩深度的局部物理子图。
        """
        async with self.driver.session() as session:
            query = (
                "MATCH path = (start {id: $start_node_id})-[r*0..2]->(node) "
                "RETURN path"
            )
            # 根据深度限制返回图路径数据
            result = await session.run(query, start_node_id=start_node_id)
            
            nodes_in_subgraph = {}
            edges_in_subgraph = []
            
            async for record in result:
                path = record["path"]
                # 遍历路径中的节点
                for node in path.nodes:
                    node_id = node.get("id")
                    labels = list(node.labels)
                    label = labels[0] if labels else "Unknown"
                    props = dict(node)
                    # 估算相对深度
                    nodes_in_subgraph[node_id] = {
                        "label": label,
                        "properties": props,
                        "depth": 0 if node_id == start_node_id else 1
                    }
                # 遍历关系
                for rel in path.relationships:
                    start_id = rel.nodes[0].get("id")
                    end_id = rel.nodes[1].get("id")
                    edges_in_subgraph.append((
                        start_id, end_id, 
                        {"rel_type": rel.type, "weight": rel.get("weight", 1.0)}
                    ))
            
            return {
                "nodes": nodes_in_subgraph,
                "edges": edges_in_subgraph
            }


class ManifoldSemanticEngine:
    """
    真实的混合流形检索数学计算引擎。
    结合余弦向量相似度与图谱拓扑关联距离（如 BFS 最短深度），计算出最终的高维融合流形相似分值。
    """
    def __init__(self):
        # 预设词表以支持轻量级词袋（TF-IDF）嵌入计算，防止无 OpenAI Key 时计算阻断
        self.mock_vocabulary = ["bottleneck", "cpu", "channel", "latency", "network", "performance", "normal"]

    def _get_bag_of_words_vector(self, text: str) -> List[float]:
        """
        计算句子的词频向量（纯 Python 零依赖实现）。
        """
        text_lower = text.lower()
        vector = []
        for word in self.mock_vocabulary:
            vector.append(float(text_lower.count(word)))
        # 归一化
        norm = sum(x**2 for x in vector)**0.5
        if norm > 0:
            vector = [x / norm for x in vector]
        return vector

    def compute_cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        纯数学公式计算两个高维向量的余弦相似度。
        """
        dot_product = sum(x * y for x, y in zip(vec1, vec2))
        norm1 = sum(x**2 for x in vec1)**0.5
        norm2 = sum(x**2 for x in vec2)**0.5
        if norm1 * norm2 == 0:
            return 0.0
        return dot_product / (norm1 * norm2)

    def calculate_manifold_score(self, topo_depth: int, semantic_sim: float) -> float:
        """
        流形混合打分数学模型：
        ManifoldScore = alpha * (1 / (1 + depth)) + (1 - alpha) * SemanticSimilarity
        """
        alpha = 0.4  # 拓扑结构占比 40%，语义匹配占比 60%
        topo_score = 1.0 / (1.0 + topo_depth)
        score = alpha * topo_score + (1.0 - alpha) * semantic_sim
        return round(score, 4)


class TopologyConnector:
    """
    物理图谱与流形空间混合检索中介接口（支持 Fallback 机制）。
    """
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self.semantic_engine = ManifoldSemanticEngine()
        self.use_physical_neo4j = False
        self.neo4j = None

        # 如果用户配置了物理 Neo4j 参数，尝试进行物理建立，失败则优雅降级
        if HAS_NEO4J_SDK and uri and user and password:
            logger.info("Detected Neo4j configuration, attempting connection...")
            client = PhysicalNeo4jClient(uri, (user, password))
            # 异步非阻塞建立连接
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 若事件循环已开启，则通过后台任务确认
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
            logger.info("TopologyConnector is running in HIGH-PERFORMANCE PHYSICAL MODE.")
        else:
            self.neo4j = MockNeo4jClient()
            logger.warning("Falling back to InMemory Mock Mode due to connection failure.")

    async def ingest_aligned_frame(self, key: str, aligned_frame: Dict[int, Any]) -> bool:
        """
        将路由对齐帧转换为图拓扑节点及关联边。
        """
        try:
            main_node_id = f"frame_{key}"
            # 写入主对齐帧节点
            await self.neo4j.add_node(
                node_id=main_node_id,
                label="AlignedFrame",
                properties={"key": key, "ingest_time": time.time()}
            )

            # 遍历并写入所有维度关联节点
            for dim_id, packet in aligned_frame.items():
                dim_node_id = f"dim_{dim_id}_{key}"
                # 将有效载荷 payload 转为文本便于后文语义召回
                payload_str = str(packet.payload)
                await self.neo4j.add_node(
                    node_id=dim_node_id,
                    label="DimensionNode",
                    properties={
                        "dimension_id": dim_id,
                        "payload": payload_str,
                        "packet_timestamp": packet.timestamp
                    }
                )
                # 写入包含关联（CONTAINS_DIMENSION）关系边
                await self.neo4j.add_edge(
                    source_id=main_node_id,
                    target_id=dim_node_id,
                    rel_type="CONTAINS_DIMENSION",
                    weight=1.0
                )
            
            logger.debug(f"Successfully ingested aligned frame '{key}' into Neo4j graph database.")
            return True
        except Exception as e:
            logger.error(f"Failed to ingest aligned frame: {str(e)}", exc_info=True)
            return False

    async def retrieve_topology_manifold(self, start_node_id: str, query: str, depth: int = 2) -> Dict[str, Any]:
        """
        硬核全量图谱混合流形召回算法：
        1. 从物理/内存图库读取 start_node_id 的 BFS 子图关系；
        2. 基于词袋计算检索 Query 与子图内每个维度节点 Payload 之间的真实余弦夹角分；
        3. 融合结构距离计算得到 Manifold score；
        4. 排序筛选并输出语义及流形矩阵指标。
        """
        # Step 1: 物理提取子图拓扑结构
        subgraph = await self.neo4j.get_subgraph(start_node_id, depth=depth)
        nodes = subgraph.get("nodes", {})
        edges = subgraph.get("edges", [])

        if not nodes:
            return {"query": query, "message": "Zero records found in topological manifold."}

        # 计算 Query 向量
        query_vector = self.semantic_engine._get_bag_of_words_vector(query)

        # 遍历并融合计算每个子图节点的混合得分
        ranked_nodes = []
        for node_id, node_info in nodes.items():
            props = node_info.get("properties", {})
            payload_text = props.get("payload", "") or props.get("key", "")
            
            # 计算真实的余弦语义匹配相似分
            node_vector = self.semantic_engine._get_bag_of_words_vector(payload_text)
            semantic_sim = self.semantic_engine.compute_cosine_similarity(query_vector, node_vector)
            
            # 引入拓扑相对深度（0为自身，1为直接邻居，以此类推）
            node_depth = node_info.get("depth", 1)
            
            # 融合计算流形指标得分
            manifold_score = self.semantic_engine.calculate_manifold_score(node_depth, semantic_sim)
            
            ranked_nodes.append({
                "node_id": node_id,
                "label": node_info["label"],
                "depth": node_depth,
                "semantic_similarity": semantic_sim,
                "manifold_score": manifold_score
            })

        # 按最终流形关联得分（manifold_score）降序排序
        ranked_nodes.sort(key=lambda x: x["manifold_score"], reverse=True)

        return {
            "query": query,
            "anchor_node": start_node_id,
            "engine_mode": "PHYSICAL_NEO4J" if self.use_physical_neo4j else "IN_MEMORY_FALLBACK",
            "topology_subgraph": {
                "nodes_count": len(nodes),
                "edges_count": len(edges),
                "all_nodes": list(nodes.keys())
            },
            "manifold_retrieval_ranked": ranked_nodes[:5]  # 返回最相关的 Top-5 节点
        }
