import asyncio
import logging
import json
from core.matrix_router import MatrixRouter
from engine.topology_connector import TopologyConnector

# 配置日志输出格式，用于观测数据管道对齐过程
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def main():
    # 1. 初始化 MatrixRouter（支持 10 个维度，对齐超时时间设为 3 秒）与 TopologyConnector
    router = MatrixRouter(num_dimensions=10, alignment_timeout=3.0)
    connector = TopologyConnector()

    # 启动多维度并发路由引擎
    await router.start()

    # 模拟多通道并行推送数据的辅助函数
    async def simulate_ingestion(key: str, complete: bool = True):
        # 如果 complete 为 True，则推送全部 10 个维度；
        # 如果为 False，则只推送前 9 个维度（dim 0-8），模拟第 10 个维度缺失以触发超时驱逐
        channels = range(10) if complete else range(9)
        tasks = []
        for i in channels:
            payload = {
                "channel_index": i,
                "signal_strength": round(i * 1.5, 2),
                "status": "normal"
            }
            # 异步将数据推入对应的维度通道
            tasks.append(router.push_data(dimension_id=i, key=key, payload=payload))
        await asyncio.gather(*tasks)

    # 2. 模拟两组不同场景的数据写入
    # 场景 A: 完整的 10 维数据对齐 (key: 'transaction_1001')
    print("\n>>> [场景 A] 注入完整 10 维数据帧 (Key: 'transaction_1001')")
    await simulate_ingestion(key="transaction_1001", complete=True)

    # 场景 B: 缺失单维度的数据，触发超时驱逐 (key: 'transaction_1002')
    print("\n>>> [场景 B] 注入不完整数据帧以触发超时驱逐 (Key: 'transaction_1002')")
    await simulate_ingestion(key="transaction_1002", complete=False)

    # 3. 异步监听对齐结果并同步至拓扑连接器
    # 期望处理 2 个帧：一个完美对齐帧，一个超时部分对齐帧
    for i in range(2):
        key, frame = await router.get_aligned_frame()
        print(f"\n[对齐队列监听到帧] Key: {key}, 维度数量: {len(frame)}/10")
        
        # 将对齐的帧数据写入模拟的 Neo4j 拓扑结构中
        await connector.ingest_aligned_frame(key, frame)

    # 4. 执行多维拓扑网络子图流形检索
    print("\n>>> [检索测试] 执行拓扑网络子图流形检索")
    query_result = await connector.retrieve_topology_manifold(
        start_node_id="frame_transaction_1001",
        query="检测当前拓扑结构中是否存在多通道并发瓶颈或性能滑坡特征。",
        depth=1
    )
    
    # 美化打印最终的流形检索结果
    print("\n[流形检索融合输出 (Topology Manifold Output)]:")
    print(json.dumps(query_result, indent=2, ensure_ascii=False))

    # 停止路由引擎工作协程
    await router.stop()

if __name__ == "__main__":
    asyncio.run(main())
