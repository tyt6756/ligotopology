import pytest
from core.matrix_router import DimensionPacket
from engine.topology_connector import TopologyConnector

@pytest.mark.asyncio
async def test_topology_ingestion_and_hybrid_retrieval():
    """
    验证 TopologyConnector 在降级（InMemory）模式下的图谱写入、子图提取和混合流形检索功能。
    """
    # 初始化连接器（默认自动 Fallback 到 InMemory）
    connector = TopologyConnector()
    
    # 模拟构建一个已对齐的帧数据 (包含 3 个维度信号)
    key = "test_trans_999"
    aligned_frame = {
        0: DimensionPacket(dimension_id=0, key=key, payload="Channel 0 reports critical CPU bottleneck"),
        1: DimensionPacket(dimension_id=1, key=key, payload="Channel 1 status is normal"),
        2: DimensionPacket(dimension_id=2, key=key, payload="Channel 2 report: network latency high")
    }
    
    # 1. 验证数据帧写入
    ingest_success = await connector.ingest_aligned_frame(key, aligned_frame)
    assert ingest_success is True
    
    # 2. 验证混合流形检索
    # 检索 Query 包含 'CPU bottleneck'
    result = await connector.retrieve_topology_manifold(
        start_node_id=f"frame_{key}",
        query="Analyze CPU bottleneck occurrences",
        depth=1
    )
    
    assert result["anchor_node"] == f"frame_{key}"
    assert result["engine_mode"] == "IN_MEMORY_FALLBACK"
    
    subgraph = result["topology_subgraph"]
    assert subgraph["nodes_count"] == 4  # 1个主帧 + 3个维度节点
    assert subgraph["edges_count"] == 3
    assert subgraph["average_degree"] == 0.75  # 3 / 4
    assert subgraph["graph_density"] == 0.5    # 2 * 3 / (4 * 3) = 0.5
    
    # 3. 验证 Top 排名打分规则
    ranked_list = result["manifold_retrieval_ranked"]
    assert len(ranked_list) > 0
    
    # 理论上 dim 0 由于包含 'CPU' & 'bottleneck'，余弦相似度必须大于 0.0，且 manifold_score 最高
    best_match = ranked_list[0]
    assert best_match["node_id"] == f"dim_0_{key}"
    assert best_match["semantic_similarity"] > 0.0
    assert best_match["manifold_score"] > 0.2  # 拓扑基准 0.2 + 语义相似分
