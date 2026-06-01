# LigoTopology

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/Python-3.9%2B-brightgreen.svg)](https://www.python.org/)

**LigoTopology** is a high-performance, asynchronous middleware engineered for multi-dimensional data alignment and topological graph manifold retrieval. It bridges the gap between high-throughput, chaotic asynchronous multi-channel data streams and structured topological knowledge representation systems (e.g., Neo4j and hybrid RAG frameworks like LightRAG).

LigoTopology offers non-blocking parallelism, deterministic sequence alignment over sliding windows, and seamless vector-topological manifold retrieval pipelines suitable for large-scale distributed systems, real-time complex event processing (CEP), and graph-enhanced agentic workflows.

---

## 🛠️ Architecture Overview

The system consists of two primary engines:

1. **Matrix Router (`core.matrix_router`)**:
   - Manages **10 independent parallel dimension channels** implemented using Python's `asyncio` loop.
   - Collects high-throughput streams and performs key-based alignment (e.g., matching across transaction IDs or sliding temporal buckets).
   - Features a thread-safe lock-free buffering architecture with an **active eviction scheduler** that prevents memory leakage by dispatching partially aligned frames upon timeout.

2. **Topology Connector (`engine.topology_connector`)**:
   - Integrates graph databases (such as Neo4j) with Graph RAG engines (such as LightRAG).
   - Projects aligned multi-dimensional frames into a coherent physical graph representation.
   - Provides a **Topological Manifold Retrieval** query interface, synthesizing graph topological contexts (subgraphs) and high-dimensional manifold vectors for precise LLM-oriented context retrieval.

```mermaid
graph TD
    subgraph Matrix Router (10-Channel Pipeline)
        In0[Dim 0 Input] --> Q0[Queue 0]
        In1[Dim 1 Input] --> Q1[Queue 1]
        In9[Dim 9 Input] --> Q9[Queue 9]
        Q0 --> Worker0[Worker 0]
        Q1 --> Worker1[Worker 1]
        Q9 --> Worker9[Worker 9]
        Worker0 & Worker1 & Worker9 --> Buff[Alignment Buffer]
        Buff -->|Full 10-Channel Match| Align[Aligned Frame]
        Buff -->|Timeout Eviction| Partial[Partial Frame]
    end
    
    subgraph Topology Connector
        Align & Partial --> Neo4j[Neo4j Client]
        Neo4j -->|Sub-graph Extraction| Manifold[Manifold Fusion Query]
        LightRAG[LightRAG Client] -->|Semantic Embedding| Manifold
        Manifold --> Out[Fused Topological Manifold Result]
    end
```

---

## 🚀 Quick Start

Here is a complete, executable demonstration showing how to stream multi-dimensional data through the **Matrix Router** and query the **Topology Connector**.

### Prerequisites

Create a virtual environment and install the required dependencies:

```bash
pip install -r requirements.txt
```

### Run Demonstration (`example.py`)

Create a script named `example.py` or run the snippet below:

```python
import asyncio
import logging
from core.matrix_router import MatrixRouter
from engine.topology_connector import TopologyConnector

# Set up logging to observe pipeline alignment behavior
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

async def main():
    # 1. Initialize the Router and Connector
    router = MatrixRouter(num_dimensions=10, alignment_timeout=3.0)
    connector = TopologyConnector()

    # Start the matrix routing workers
    await router.start()

    # Define a helper function to simulate concurrent ingestion for a specific key
    async def simulate_ingestion(key: str, complete: bool = True):
        # If 'complete' is True, we push to all 10 channels. Otherwise, we omit channel 9 to trigger timeout.
        channels = range(10) if complete else range(9)
        tasks = []
        for i in channels:
            payload = {"sensor_val": i * 1.5, "metric": "cpu_load" if i % 2 == 0 else "io_wait"}
            tasks.append(router.push_data(dimension_id=i, key=key, payload=payload))
        await asyncio.gather(*tasks)

    # 2. Spawn concurrent data feeds
    print("\n--- Push Complete Data Frame (Key: 'transaction_1001') ---")
    await simulate_ingestion(key="transaction_1001", complete=True)

    print("\n--- Push Incomplete Data Frame to trigger Eviction (Key: 'transaction_1002') ---")
    await simulate_ingestion(key="transaction_1002", complete=False)

    # 3. Pull aligned frames from the Matrix Router output queue and ingest into Topology Connector
    # We will process 2 frames (one aligned, one timeout partial)
    for _ in range(2):
        key, frame = await router.get_aligned_frame()
        # Ingest the frame into the Mock Neo4j Subgraph
        await connector.ingest_aligned_frame(key, frame)

    # 4. Perform a Topological Manifold Retrieval query
    print("\n--- Executing Topological Manifold Retrieval ---")
    query_result = await connector.retrieve_topology_manifold(
        start_node_id="frame_transaction_1001",
        query="Analyze the potential network bottleneck or CPU spike characteristics across channels.",
        depth=1
    )
    
    print("\n[Manifold Query Output Result]:")
    import json
    print(json.dumps(query_result, indent=2, ensure_ascii=False))

    # Stop the Router Engine workers
    await router.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📘 API Reference

### `core.matrix_router.MatrixRouter`

- `__init__(num_dimensions: int = 10, alignment_timeout: float = 5.0)`: Instantiates a new router.
- `start()`: Launches the background workers listening to the dimension queues.
- `push_data(dimension_id: int, key: str, payload: Any)`: Non-blocking ingestion.
- `get_aligned_frame()`: Awaits the next output queue packet.
- `stop()`: Shuts down the router safely.

### `engine.topology_connector.TopologyConnector`

- `ingest_aligned_frame(key: str, aligned_frame: Dict[int, DimensionPacket]) -> bool`: Persists the multi-dimensional structure to the simulated Neo4j backend.
- `retrieve_topology_manifold(start_node_id: str, query: str, depth: int = 2) -> Dict[str, Any]`: Executes BFS subgraph aggregation, communicates with the Mock LightRAG server, and forms the manifold context vectors.

---

## 📄 License

LigoTopology is licensed under the Apache 2.0 License.
