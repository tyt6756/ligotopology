import asyncio
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
