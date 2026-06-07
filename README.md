# LigoTopology

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/Python-3.9%2B-brightgreen.svg)](https://www.python.org/)
[![CI Status](https://github.com/tyt6756/ligotopology/actions/workflows/ci.yml/badge.svg)](https://github.com/tyt6756/ligotopology/actions)

LigoTopology is a lightweight, high-performance asynchronous middleware designed for multi-channel streaming data alignment and hybrid vector-topological GraphRAG retrieval. 

It serves as a non-blocking buffer and synchronization layer that aligns parallel streams of heterogeneous data before injecting them into a graph database, ensuring data consistency and enabling context-aware semantic retrieval for downstream AI agents.

---

## 🌍 Ecosystem & Practical Value

In multi-source data processing systems—such as **Open Source Intelligence (OSINT)** networks, **location-based tourism tracking**, or **distributed sensor monitoring**—data streams from multiple APIs arrive concurrently, asynchronously, and out of order.

Without middleware like LigoTopology:
- Directly writing multi-channel streams into a graph database (e.g., Neo4j) causes frequent write locks, connection bottlenecks, and inconsistent nodes/edges.
- Multi-agent reasoners struggle to query real-time status due to high IO overhead and lack of unified context alignment.

### How LigoTopology Solves This:
1. **Asynchronous Multi-Channel Buffering**: Captures up to 10 independent streams (e.g., traffic updates, social media posts, weather conditions) in parallel memory queues.
2. **Deterministic Time-Window Alignment**: Groups incoming events under a shared correlation key (such as a transaction ID or timeslot) using `MatrixRouter`. If a channel fails to deliver within the timeout window, the router evicts the partial frame, preventing memory leaks.
3. **Batch Graph Ingestion**: Ingests aligned multi-dimensional frames into Neo4j in batch transactions, cutting transaction overhead.
4. **Hybrid GraphRAG Retrieval**: Combines topological graph distance (e.g., shortest BFS path from the queried entity) and semantic similarity (cosine similarity of payloads) using `TopologyConnector`, allowing agents to retrieve context with high relevance.

---

## 📊 Performance & Reproducible Benchmarks

LigoTopology features an optimized memory model that handles high-concurrency ingestion workloads. 

### Stress Test Configuration
- **Workload**: 10 parallel channels processing 1,000 packets per channel (10,000 packets total)
- **Environment**: Single CPU instance, Python 3.13, Windows 11 / Linux Ubuntu

### Benchmark Metrics

| Metric | Measured Result | Performance Characteristics |
| :--- | :--- | :--- |
| **Throughput (QPS)** | **62,926.23 packets/s** | High-speed concurrent alignment |
| **Total Frames Aligned** | **1,000 frames** | Complete synchronization of 10 dimensions |
| **Processing Duration** | **0.16 seconds** | Negligible queuing overhead |
| **Ingestion Success Rate** | **100.0%** | Zero packet drop under steady-state load |

### How to Reproduce the Benchmark
Run the built-in stress test script to measure throughput on your local hardware:
```bash
# Set PYTHONPATH to root and run the profiling test
$env:PYTHONPATH="."  # On Linux/macOS: export PYTHONPATH="."
python tests/stress_test.py
```

---

## 🛠️ Hybrid Retrieval Scoring Model

`TopologyConnector` ranks subgraphs using a hybrid formula balancing structural proximity and text similarity:

$$\text{Score} = \alpha \cdot \left(\frac{1}{1 + \text{depth}}\right) + (1 - \alpha) \cdot \text{SemanticSimilarity}$$

Where:
* $\alpha = 0.4$ (graph structure weight vs. text cosine similarity).
* $\text{depth}$: BFS hop count from the queried anchor frame node.
* $\text{SemanticSimilarity}$: Vector cosine similarity computed via TF-IDF bag-of-words mapping.

---

## 🚀 Quick Start

### Installation

Install LigoTopology in editable developer mode:
```bash
pip install -e .[dev]
```

### Execution Example (`example.py`)

Run the complete pipeline demonstration:
```bash
python example.py
```

Expected terminal output:
```text
>>> [Scenario A] Injecting complete 10-dimensional data frame (Key: 'transaction_1001')
>>> [Scenario B] Injecting incomplete data frame to trigger timeout eviction (Key: 'transaction_1002')

[Queue Monitor] Aligned Frame Detected -> Key: transaction_1001, Dimensions: 10/10
[Queue Monitor] Aligned Frame Detected -> Key: transaction_1002, Dimensions: 9/10

>>> [Query Test] Executing multi-dimensional topological graph manifold retrieval...

[Manifold Query Output Result]:
{
  "query": "Analyze the CPU bottleneck and performance latency characteristics.",
  "anchor_node": "frame_transaction_1001",
  "engine_mode": "IN_MEMORY_FALLBACK",
  "topology_subgraph": {
    "nodes_count": 11,
    "edges_count": 10,
    "all_nodes": [...]
  },
  "manifold_retrieval_ranked": [
    {
      "node_id": "dim_3_transaction_1001",
      "depth": 1,
      "semantic_similarity": 0.5669,
      "manifold_score": 0.5402
    },
    ...
  ]
}
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

- `__init__(uri: str = None, user: str = None, password: str = None)`: Instantiates a connector. Auto-falls back to high-performance InMemory mode if configurations are missing.
- `ingest_aligned_frame(key: str, aligned_frame: Dict[int, DimensionPacket]) -> bool`: Persists the multi-dimensional structure to the Neo4j backend.
- `retrieve_topology_manifold(start_node_id: str, query: str, depth: int = 2) -> Dict[str, Any]`: Executes BFS subgraph aggregation, computes cosine similarity, and forms the manifold context vectors.

---

## 📄 License

LigoTopology is licensed under the Apache 2.0 License.
