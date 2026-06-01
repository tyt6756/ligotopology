import asyncio
import time
import random
import logging
from core.matrix_router import MatrixRouter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StressTest")

async def load_generator(router: MatrixRouter, dimension_id: int, num_packets: int):
    """
    模拟单个通道的高并发数据发生器。
    """
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
