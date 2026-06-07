import asyncio
import pytest
from core.matrix_router import MatrixRouter

@pytest.mark.asyncio
async def test_eviction_on_partial_data():
    """
    验证当部分维度缺失时，MatrixRouter 是否能在超时设定后正确发出部分对齐的数据帧。
    """
    router = MatrixRouter(num_dimensions=10, alignment_timeout=0.5)
    await router.start()
    
    key = "test_eviction_key"
    # 仅推送前 8 个维度的数据，留空 dim 8 和 dim 9
    for i in range(8):
        await router.push_data(dimension_id=i, key=key, payload=f"data_{i}")
        
    await asyncio.sleep(0.7)
    
    out_key, frame = await router.get_aligned_frame()
    assert out_key == key
    assert len(frame) == 8
    assert 8 not in frame
    assert 9 not in frame
    
    await router.stop()


@pytest.mark.asyncio
async def test_extreme_lock_concurrency():
    """
    边界竞争测试：在相同 Key 下进行极高频的并发写入，验证 MatrixRouter 异步锁的死锁预防和资源分配。
    """
    router = MatrixRouter(num_dimensions=10, alignment_timeout=1.0)
    await router.start()
    
    key = "concurrency_race_key"
    # 异步并发压入 10 个维度的数据包
    tasks = [
        router.push_data(dimension_id=i, key=key, payload=f"race_payload_{i}")
        for i in range(10)
    ]
    await asyncio.gather(*tasks)
    
    out_key, frame = await router.get_aligned_frame()
    assert out_key == key
    assert len(frame) == 10
    
    await router.stop()
