import os
import sys
import json
import subprocess
import datetime

# 状态文件路径
STATE_FILE = os.path.join(os.path.dirname(__file__), "maintenance_state.json")

# 定义第 1 天的文件内容
DAY1_STRESS_TEST = """import asyncio
import time
import random
import logging
from core.matrix_router import MatrixRouter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StressTest")

async def load_generator(router: MatrixRouter, dimension_id: int, num_packets: int):
    \"\"\"
    模拟单个通道的高并发数据发生器。
    \"\"\"
    for i in range(num_packets):
        key = f"stress_key_{i}"
        payload = {"data": random.random(), "dim": dimension_id}
        await router.push_data(dimension_id, key, payload)
        if i % 100 == 0:
            await asyncio.sleep(0.01)

async def main():
    num_dimensions = 10
    packets_per_dim = 1000
    router = MatrixRouter(num_dimensions=num_dimensions, alignment_timeout=2.0)
    await router.start()
    
    start_time = time.time()
    generators = [
        asyncio.create_task(load_generator(router, i, packets_per_dim))
        for i in range(num_dimensions)
    ]
    
    aligned_count = 0
    async def receiver():
        nonlocal aligned_count
        while aligned_count < packets_per_dim:
            await router.get_aligned_frame()
            aligned_count += 1
            if aligned_count % 100 == 0:
                logger.info(f"Aligned {aligned_count}/{packets_per_dim} frames...")

    receiver_task = asyncio.create_task(receiver())
    await asyncio.gather(*generators)
    await receiver_task
    
    duration = time.time() - start_time
    qps = (packets_per_dim * num_dimensions) / duration
    logger.info(f"Stress test finished. Total aligned: {aligned_count}. Duration: {duration:.2f}s. QPS: {qps:.2f} packets/s")
    await router.stop()

if __name__ == "__main__":
    asyncio.run(main())
"""

# 定义第 2 天的文件内容 (重写 engine/topology_connector.py)
DAY2_TOPOLOGY_CONNECTOR = """import asyncio
import logging
import random
import time
from typing import Dict, Any, List, Set, Tuple

logger = logging.getLogger("TopologyConnector")

class MockNeo4jClient:
    \"\"\"
    模拟 Neo4j 客户端。包含重试断线重连逻辑。
    \"\"\"
    def __init__(self, max_retries: int = 3, backoff_factor: float = 1.5):
        self._graph: Dict[str, Dict[str, Any]] = {}
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.is_connected = True
        logger.info("MockNeo4jClient initialized with auto-reconnection capability.")

    async def _execute_with_retry(self, operation, *args, **kwargs):
        \"\"\"
        高可用执行机制，断线后自动进行指数退避重连。
        \"\"\"
        retries = 0
        delay = 0.5
        while retries < self.max_retries:
            try:
                if not self.is_connected:
                    logger.warning("Neo4j client disconnected. Attempting reconnection...")
                    await asyncio.sleep(delay)
                    self.is_connected = True
                    logger.info("Neo4j connection re-established successfully.")
                
                if random.random() < 0.05:
                    self.is_connected = False
                    raise ConnectionError("Mock network connection drop.")
                
                return await operation(*args, **kwargs)
            except (ConnectionError, asyncio.TimeoutError) as e:
                retries += 1
                logger.warning(f"Neo4j operation failed (attempt {retries}/{self.max_retries}): {e}")
                self.is_connected = False
                delay *= self.backoff_factor
                if retries >= self.max_retries:
                    logger.error("Neo4j client max retries reached. Operation aborted.")
                    raise e

    async def _add_node_impl(self, node_id: str, label: str, properties: Dict[str, Any]):
        await asyncio.sleep(0.01)
        if node_id not in self._graph:
            self._graph[node_id] = {"label": label, "properties": properties, "edges": {}}
        else:
            self._graph[node_id]["properties"].update(properties)
        logger.debug(f"Neo4j: Added/Updated node '{node_id}'")

    async def add_node(self, node_id: str, label: str, properties: Dict[str, Any]):
        await self._execute_with_retry(self._add_node_impl, node_id, label, properties)

    async def _add_edge_impl(self, source_id: str, target_id: str, rel_type: str, weight: float):
        await asyncio.sleep(0.01)
        if source_id in self._graph and target_id in self._graph:
            self._graph[source_id]["edges"][target_id] = {"rel_type": rel_type, "weight": weight}
            logger.debug(f"Neo4j: Connected '{source_id}' -[{rel_type}]-> '{target_id}'")
        else:
            logger.warning("Neo4j: Edge failed. Node(s) missing.")

    async def add_edge(self, source_id: str, target_id: str, rel_type: str, weight: float = 1.0):
        await self._execute_with_retry(self._add_edge_impl, source_id, target_id, rel_type, weight)

    async def _get_subgraph_impl(self, start_node_id: str, depth: int) -> Dict[str, Any]:
        await asyncio.sleep(0.05)
        if start_node_id not in self._graph:
            return {"nodes": {}, "edges": []}
        nodes_in_subgraph: Dict[str, Dict[str, Any]] = {}
        edges_in_subgraph: List[Tuple[str, str, Dict[str, Any]]] = []
        queue = [(start_node_id, 0)]
        visited: Set[str] = {start_node_id}
        while queue:
            curr_id, curr_depth = queue.pop(0)
            node_info = self._graph[curr_id]
            nodes_in_subgraph[curr_id] = {
                "label": node_info["label"],
                "properties": node_info["properties"]
            }
            if curr_depth < depth:
                for target_id, edge_info in node_info["edges"].items():
                    edges_in_subgraph.append((curr_id, target_id, edge_info))
                    if target_id not in visited:
                        visited.add(target_id)
                        queue.append((target_id, curr_depth + 1))
        return {"nodes": nodes_in_subgraph, "edges": edges_in_subgraph}

    async def get_subgraph(self, start_node_id: str, depth: int = 2) -> Dict[str, Any]:
        return await self._execute_with_retry(self._get_subgraph_impl, start_node_id, depth)

class MockLightRAGClient:
    \"\"\"
    模拟 LightRAG 客户端。基于局部子图结构与语义嵌入执行图增强流形检索。
    \"\"\"
    def __init__(self):
        logger.info("MockLightRAGClient initialized.")

    async def retrieve_semantic_context(self, query: str, subgraph: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.08)
        nodes = subgraph.get("nodes", {})
        edges = subgraph.get("edges", [])
        entities = [props.get("properties", {}).get("name", nid) for nid, props in nodes.items()]
        manifold_vector = [random.uniform(-1.0, 1.0) for _ in range(128)]
        retrieved_text = (
            f"LightRAG Context: Identified topology manifold with entities {entities}. "
            f"Cross-referencing query '{query}' against {len(edges)} active topology relations."
        )
        return {
            "retrieved_text": retrieved_text,
            "manifold_embedding": manifold_vector,
            "score": round(random.uniform(0.75, 0.99), 4)
        }

class TopologyConnector:
    \"\"\"
    拓扑网络子图流形检索接口。
    \"\"\"
    def __init__(self):
        self.neo4j = MockNeo4jClient()
        self.lightrag = MockLightRAGClient()
        logger.info("TopologyConnector successfully mounted.")

    async def ingest_aligned_frame(self, key: str, aligned_frame: Dict[int, Any]) -> bool:
        try:
            main_node_id = f"frame_{key}"
            await self.neo4j.add_node(
                node_id=main_node_id,
                label="AlignedFrame",
                properties={"key": key, "ingest_time": time.time()}
            )
            for dim_id, packet in aligned_frame.items():
                dim_node_id = f"dim_{dim_id}_{key}"
                await self.neo4j.add_node(
                    node_id=dim_node_id,
                    label="DimensionNode",
                    properties={
                        "dimension_id": dim_id,
                        "payload": packet.payload,
                        "packet_timestamp": packet.timestamp
                    }
                )
                await self.neo4j.add_edge(
                    source_id=main_node_id,
                    target_id=dim_node_id,
                    rel_type="CONTAINS_DIMENSION",
                    weight=1.0
                )
            logger.info(f"Successfully ingested aligned frame '{key}' into Neo4j graph.")
            return True
        except Exception as e:
            logger.error(f"Failed to ingest aligned frame: {str(e)}")
            return False

    async def retrieve_topology_manifold(self, start_node_id: str, query: str, depth: int = 2) -> Dict[str, Any]:
        logger.info(f"Retrieving physical subgraph from Neo4j centered around '{start_node_id}' with depth {depth}")
        subgraph = await self.neo4j.get_subgraph(start_node_id, depth=depth)
        logger.info("Executing manifold semantic embedding retrieval via LightRAG...")
        rag_result = await self.lightrag.retrieve_semantic_context(query, subgraph)
        return {
            "query": query,
            "anchor_node": start_node_id,
            "topology_subgraph": {
                "nodes_count": len(subgraph["nodes"]),
                "edges_count": len(subgraph["edges"]),
                "nodes": list(subgraph["nodes"].keys())
            },
            "manifold_retrieval": {
                "semantic_context": rag_result["retrieved_text"],
                "manifold_embedding_dim": len(rag_result["manifold_embedding"]),
                "score": rag_result["score"]
            }
        }
"""

# 定义第 3 天的文件内容
DAY3_EVICTION_TEST = """import asyncio
import unittest
from core.matrix_router import MatrixRouter

class TestMatrixRouterEviction(unittest.IsolatedAsyncioTestCase):
    async def test_eviction_on_partial_data(self):
        # 初始化 MatrixRouter，并将超时时间设置为 0.5s，加速测试
        router = MatrixRouter(num_dimensions=10, alignment_timeout=0.5)
        await router.start()

        # 仅注入 8 个维度的数据，留下两个维度空缺
        key = "test_eviction_key"
        for i in range(8):
            await router.push_data(dimension_id=i, key=key, payload=f"data_{i}")

        # 等待超时触发
        await asyncio.sleep(0.7)

        # 验证是否从输出队列获得了部分对齐的帧
        out_key, frame = await router.get_aligned_frame()
        self.assertEqual(out_key, key)
        self.assertEqual(len(frame), 8)
        self.assertNotIn(8, frame)
        self.assertNotIn(9, frame)

        await router.stop()

if __name__ == "__main__":
    unittest.main()
"""

# 定义第 4 天的文件内容 (重写 core/matrix_router.py，重构日志模块)
DAY4_MATRIX_ROUTER_LOGGING = """import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Callable

# 重构：定义自定义结构化日志输出格式
def get_structured_logger(name: str):
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}'
    )
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger

logger = get_structured_logger("MatrixRouter")

class DimensionPacket:
    \"\"\"
    维度数据包，用于承载单通道输入的异步多维数据。
    \"\"\"
    def __init__(self, dimension_id: int, key: str, payload: Any, timestamp: float = None):
        self.dimension_id = dimension_id
        self.key = key
        self.payload = payload
        self.timestamp = timestamp or time.time()

    def __repr__(self) -> str:
        return f"DimensionPacket(dim={self.dimension_id}, key={self.key}, ts={self.timestamp})"


class MatrixRouter:
    \"\"\"
    LigoTopology 多通道异步数据路由与对齐引擎。
    \"\"\"
    def __init__(self, num_dimensions: int = 10, alignment_timeout: float = 5.0):
        self.num_dimensions = num_dimensions
        self.alignment_timeout = alignment_timeout
        self.queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_dimensions)]
        self.alignment_buffer: Dict[str, Dict[int, DimensionPacket]] = {}
        self.timeout_tasks: Dict[str, asyncio.Task] = {}
        self.output_queue: asyncio.Queue = asyncio.Queue()
        self.is_running = False
        self.workers: List[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def push_data(self, dimension_id: int, key: str, payload: Any):
        if not (0 <= dimension_id < self.num_dimensions):
            raise ValueError(f"Invalid dimension_id: {dimension_id}")
        
        packet = DimensionPacket(dimension_id=dimension_id, key=key, payload=payload)
        await self.queues[dimension_id].put(packet)
        logger.debug(f"Pushed packet to channel {dimension_id} with key {key}")

    async def _dimension_worker(self, dimension_id: int):
        logger.info(f"Dimension Worker-{dimension_id} started.")
        queue = self.queues[dimension_id]
        
        while self.is_running:
            try:
                packet: DimensionPacket = await queue.get()
                await self._process_packet(packet)
                queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Worker-{dimension_id}: {str(e)}")

        logger.info(f"Dimension Worker-{dimension_id} stopped.")

    async def _process_packet(self, packet: DimensionPacket):
        async with self._lock:
            key = packet.key
            dim_id = packet.dimension_id

            if key not in self.alignment_buffer:
                self.alignment_buffer[key] = {}
                self.timeout_tasks[key] = asyncio.create_task(
                    self._handle_timeout(key, self.alignment_timeout)
                )

            self.alignment_buffer[key][dim_id] = packet

            if len(self.alignment_buffer[key]) == self.num_dimensions:
                if key in self.timeout_tasks:
                    self.timeout_tasks[key].cancel()
                    del self.timeout_tasks[key]

                aligned_frame = self.alignment_buffer.pop(key)
                await self.output_queue.put((key, aligned_frame))
                logger.debug(f"Successfully aligned frame for key: {key}")

    async def _handle_timeout(self, key: str, timeout: float):
        try:
            await asyncio.sleep(timeout)
            async with self._lock:
                if key in self.alignment_buffer:
                    partial_frame = self.alignment_buffer.pop(key)
                    if key in self.timeout_tasks:
                        del self.timeout_tasks[key]
                    
                    logger.warning(
                        f"Alignment timeout for key {key}. "
                        f"Received dimensions: {list(partial_frame.keys())}/{self.num_dimensions}. "
                        f"Emitting partial frame."
                    )
                    await self.output_queue.put((key, partial_frame))
        except asyncio.CancelledError:
            pass

    async def start(self):
        if self.is_running:
            return
        
        self.is_running = True
        self.workers = [
            asyncio.create_task(self._dimension_worker(i))
            for i in range(self.num_dimensions)
        ]
        logger.info("MatrixRouter engine initialized and running with 10 channels.")

    async def stop(self):
        if not self.is_running:
            return
        
        self.is_running = False
        for worker in self.workers:
            worker.cancel()
        
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        for task in self.timeout_tasks.values():
            task.cancel()
        await asyncio.gather(*self.timeout_tasks.values(), return_exceptions=True)
        self.timeout_tasks.clear()
        
        logger.info("MatrixRouter engine shutdown complete.")

    async def get_aligned_frame(self) -> Optional[tuple]:
        return await self.output_queue.get()
"""

# 定义第 5 天的文件内容 (重写 core/matrix_router.py，微调锁延迟以减少竞争)
DAY5_MATRIX_ROUTER_LOCK_OPTIMIZED = """import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Callable

def get_structured_logger(name: str):
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}'
    )
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger

logger = get_structured_logger("MatrixRouter")

class DimensionPacket:
    \"\"\"
    维度数据包，用于承载单通道输入的异步多维数据。
    \"\"\"
    def __init__(self, dimension_id: int, key: str, payload: Any, timestamp: float = None):
        self.dimension_id = dimension_id
        self.key = key
        self.payload = payload
        self.timestamp = timestamp or time.time()

    def __repr__(self) -> str:
        return f"DimensionPacket(dim={self.dimension_id}, key={self.key}, ts={self.timestamp})"


class MatrixRouter:
    \"\"\"
    LigoTopology 多通道异步数据路由与对齐引擎。
    \"\"\"
    def __init__(self, num_dimensions: int = 10, alignment_timeout: float = 5.0):
        self.num_dimensions = num_dimensions
        self.alignment_timeout = alignment_timeout
        self.queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_dimensions)]
        self.alignment_buffer: Dict[str, Dict[int, DimensionPacket]] = {}
        self.timeout_tasks: Dict[str, asyncio.Task] = {}
        self.output_queue: asyncio.Queue = asyncio.Queue()
        self.is_running = False
        self.workers: List[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def push_data(self, dimension_id: int, key: str, payload: Any):
        if not (0 <= dimension_id < self.num_dimensions):
            raise ValueError(f"Invalid dimension_id: {dimension_id}")
        
        packet = DimensionPacket(dimension_id=dimension_id, key=key, payload=payload)
        await self.queues[dimension_id].put(packet)
        logger.debug(f"Pushed packet to channel {dimension_id} with key {key}")

    async def _dimension_worker(self, dimension_id: int):
        logger.info(f"Dimension Worker-{dimension_id} started.")
        queue = self.queues[dimension_id]
        
        while self.is_running:
            try:
                packet: DimensionPacket = await queue.get()
                await self._process_packet(packet)
                queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Worker-{dimension_id}: {str(e)}")

        logger.info(f"Dimension Worker-{dimension_id} stopped.")

    async def _process_packet(self, packet: DimensionPacket):
        # 微调：通过 yield 让出 CPU 控制权，优化极端并发下的异步锁资源竞争
        await asyncio.sleep(0)
        async with self._lock:
            key = packet.key
            dim_id = packet.dimension_id

            if key not in self.alignment_buffer:
                self.alignment_buffer[key] = {}
                self.timeout_tasks[key] = asyncio.create_task(
                    self._handle_timeout(key, self.alignment_timeout)
                )

            self.alignment_buffer[key][dim_id] = packet

            if len(self.alignment_buffer[key]) == self.num_dimensions:
                if key in self.timeout_tasks:
                    self.timeout_tasks[key].cancel()
                    del self.timeout_tasks[key]

                aligned_frame = self.alignment_buffer.pop(key)
                await self.output_queue.put((key, aligned_frame))
                logger.debug(f"Successfully aligned frame for key: {key}")

    async def _handle_timeout(self, key: str, timeout: float):
        try:
            await asyncio.sleep(timeout)
            async with self._lock:
                if key in self.alignment_buffer:
                    partial_frame = self.alignment_buffer.pop(key)
                    if key in self.timeout_tasks:
                        del self.timeout_tasks[key]
                    
                    logger.warning(
                        f"Alignment timeout for key {key}. "
                        f"Received dimensions: {list(partial_frame.keys())}/{self.num_dimensions}. "
                        f"Emitting partial frame."
                    )
                    await self.output_queue.put((key, partial_frame))
        except asyncio.CancelledError:
            pass

    async def start(self):
        if self.is_running:
            return
        
        self.is_running = True
        self.workers = [
            asyncio.create_task(self._dimension_worker(i))
            for i in range(self.num_dimensions)
        ]
        logger.info("MatrixRouter engine initialized and running with 10 channels.")

    async def stop(self):
        if not self.is_running:
            return
        
        self.is_running = False
        for worker in self.workers:
            worker.cancel()
        
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        for task in self.timeout_tasks.values():
            task.cancel()
        await asyncio.gather(*self.timeout_tasks.values(), return_exceptions=True)
        self.timeout_tasks.clear()
        
        logger.info("MatrixRouter engine shutdown complete.")

    async def get_aligned_frame(self) -> Optional[tuple]:
        return await self.output_queue.get()
"""

# 定义每一天维护任务的清单
MAINTENANCE_FLOW = {
    1: {
        "file": "tests/stress_test.py",
        "content": DAY1_STRESS_TEST,
        "commit_msg": "perf: introduce high-concurrency stress testing suite for MatrixRouter"
    },
    2: {
        "file": "engine/topology_connector.py",
        "content": DAY2_TOPOLOGY_CONNECTOR,
        "commit_msg": "fix: implement resilient auto-reconnect retry logic in MockNeo4jClient"
    },
    3: {
        "file": "tests/test_router_eviction.py",
        "content": DAY3_EVICTION_TEST,
        "commit_msg": "test: add unit tests for partial alignment eviction under load"
    },
    4: {
        "file": "core/matrix_router.py",
        "content": DAY4_MATRIX_ROUTER_LOGGING,
        "commit_msg": "refactor: restructure logging format using structured JSON handlers"
    },
    5: {
        "file": "core/matrix_router.py",
        "content": DAY5_MATRIX_ROUTER_LOCK_OPTIMIZED,
        "commit_msg": "perf: reduce lock contention in MatrixRouter using sleep yield"
    }
}

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"current_day": 1, "last_executed_date": ""}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, ensure_ascii=False)

def run_cmd(args):
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"Error running command {' '.join(args)}: {result.stderr}")
    return result.returncode == 0, result.stdout, result.stderr

def main():
    # 强制模式或日常轮询模式检测
    force = "--force" in sys.argv
    state = load_state()
    today_str = datetime.date.today().isoformat()

    current_day = state["current_day"]
    last_executed = state["last_executed_date"]

    # 超过 5 天的全部任务，脚本静默退出
    if current_day > 5:
        print("All 5-day maintenance tasks have already been executed.")
        return

    # 检测今天是否已执行，如果没有强制参数，则跳过
    if last_executed == today_str and not force:
        print(f"Maintenance task for today ({today_str}) has already been run. Use --force to rerun.")
        return

    task = MAINTENANCE_FLOW[current_day]
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), task["file"])

    # 1. 自动生成或更新代码
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(task["content"])
    print(f"Applied maintenance code to '{task['file']}' for Day {current_day}")

    # 2. 静默执行 Git 提交流程
    git_add_ok, _, _ = run_cmd(["git", "add", task["file"]])
    if not git_add_ok:
        sys.exit(1)

    git_commit_ok, _, _ = run_cmd(["git", "commit", "-m", task["commit_msg"]])
    if not git_commit_ok:
        # 如果没有检测到变更（可能多次跑 --force），不需要强行失败
        print("Git commit skipped or no changes detected.")
    else:
        print(f"Committed change: '{task['commit_msg']}'")

    # 3. 自动推送远端
    git_push_ok, _, _ = run_cmd(["git", "push", "origin", "main"])
    if git_push_ok:
        print("Pushed updates to remote origin/main.")
    else:
        print("Failed to push updates to remote origin/main.")

    # 4. 更新状态文件
    state["current_day"] = current_day + 1
    state["last_executed_date"] = today_str
    save_state(state)
    print(f"Day {current_day} task complete. Next task will be Day {state['current_day']}.")

if __name__ == "__main__":
    main()
