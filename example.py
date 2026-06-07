import asyncio
import logging
import json
from core.matrix_router import MatrixRouter
from engine.topology_connector import TopologyConnector

# Set up logging format to observe pipeline alignment behavior
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def main():
    # 1. Initialize the Router and Connector
    # TopologyConnector auto-falls back to high-performance InMemory mode if configurations are missing
    router = MatrixRouter(num_dimensions=10, alignment_timeout=3.0)
    connector = TopologyConnector()

    # Start the matrix routing workers
    await router.start()

    # Helper function to simulate concurrent ingestion for a specific key
    async def simulate_ingestion(key: str, complete: bool = True):
        # If 'complete' is True, we push to all 10 channels. 
        # Otherwise, we omit channel 9 to trigger timeout eviction.
        channels = range(10) if complete else range(9)
        tasks = []
        for i in channels:
            # Inject keywords like 'bottleneck', 'cpu', 'latency' to trigger semantic match
            status_desc = "normal status"
            if i == 3:
                status_desc = "critical CPU bottleneck and high latency"
            elif i == 7:
                status_desc = "network performance degradation and bottleneck"

            payload = {
                "channel_index": i,
                "signal_strength": round(i * 1.5, 2),
                "payload": f"Channel {i} warning report: {status_desc}"
            }
            # Push data into corresponding dimension queues asynchronously
            tasks.append(router.push_data(dimension_id=i, key=key, payload=payload))
        await asyncio.gather(*tasks)

    # 2. Spawn concurrent data feeds
    # Scenario A: Complete 10-dimensional alignment (key: 'transaction_1001')
    print("\n>>> [Scenario A] Injecting complete 10-dimensional data frame (Key: 'transaction_1001')")
    await simulate_ingestion(key="transaction_1001", complete=True)

    # Scenario B: Incomplete data frame triggering Eviction (key: 'transaction_1002')
    print("\n>>> [Scenario B] Injecting incomplete data frame to trigger timeout eviction (Key: 'transaction_1002')")
    await simulate_ingestion(key="transaction_1002", complete=False)

    # 3. Pull aligned frames from the Matrix Router output queue and ingest into Topology Connector
    # We will process 2 frames (one fully aligned, one timeout partial)
    for i in range(2):
        key, frame = await router.get_aligned_frame()
        print(f"\n[Queue Monitor] Aligned Frame Detected -> Key: {key}, Dimensions: {len(frame)}/10")
        
        # Ingest the frame into the Mock Neo4j Subgraph
        await connector.ingest_aligned_frame(key, frame)

    # 4. Perform a Topological Manifold Retrieval query
    print("\n>>> [Query Test] Executing multi-dimensional topological graph manifold retrieval...")
    query_result = await connector.retrieve_topology_manifold(
        start_node_id="frame_transaction_1001",
        query="Analyze the CPU bottleneck and performance latency characteristics.",
        depth=1
    )
    
    # Pretty print the final manifold retrieval results
    print("\n[Manifold Query Output Result]:")
    print(json.dumps(query_result, indent=2, ensure_ascii=False))

    # Stop the Router Engine workers
    await router.stop()

if __name__ == "__main__":
    asyncio.run(main())
