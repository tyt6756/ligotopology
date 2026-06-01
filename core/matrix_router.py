import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Callable

# 初始化日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("MatrixRouter")

class DimensionPacket:
    """
    维度数据包，用于承载单通道输入的异步多维数据。
    """
    def __init__(self, dimension_id: int, key: str, payload: Any, timestamp: float = None):
        self.dimension_id = dimension_id  # 维度通道标识，范围 [0, 9]
        self.key = key                    # 用于多维对齐的唯一键（例如序列号或时间窗哈希）
        self.payload = payload            # 承载的数据负载
        self.timestamp = timestamp or time.time()  # 接收/产生时间戳

    def __repr__(self) -> str:
        return f"DimensionPacket(dim={self.dimension_id}, key={self.key}, ts={self.timestamp})"


class MatrixRouter:
    """
    LigoTopology 多通道异步数据路由与对齐引擎。
    支持 10 个独立维度通道的并行注入，通过非阻塞异步管道对具有相同对齐键（key）的数据进行多路复用与对齐。
    """
    def __init__(self, num_dimensions: int = 10, alignment_timeout: float = 5.0):
        self.num_dimensions = num_dimensions
        self.alignment_timeout = alignment_timeout
        
        # 10 个维度的异步输入通道队列
        self.queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_dimensions)]
        
        # 对齐缓冲区： key -> { dimension_id -> DimensionPacket }
        self.alignment_buffer: Dict[str, Dict[int, DimensionPacket]] = {}
        
        # 保存每个 key 对应的超时定时任务，防止由于单通道数据缺失导致的内存泄漏
        self.timeout_tasks: Dict[str, asyncio.Task] = {}
        
        # 已对齐数据的输出队列
        self.output_queue: asyncio.Queue = asyncio.Queue()
        
        # 引擎状态控制
        self.is_running = False
        self.workers: List[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def push_data(self, dimension_id: int, key: str, payload: Any):
        """
        向指定维度通道异步推送数据包。
        """
        if not (0 <= dimension_id < self.num_dimensions):
            raise ValueError(f"Invalid dimension_id: {dimension_id}. Must be in [0, {self.num_dimensions - 1}].")
        
        packet = DimensionPacket(dimension_id=dimension_id, key=key, payload=payload)
        await self.queues[dimension_id].put(packet)
        logger.debug(f"Pushed packet to channel {dimension_id} with key {key}")

    async def _dimension_worker(self, dimension_id: int):
        """
        维度通道工作协程，并行处理单个维度的输入数据。
        """
        logger.info(f"Dimension Worker-{dimension_id} started.")
        queue = self.queues[dimension_id]
        
        while self.is_running:
            try:
                # 异步等待并获取通道中的数据包
                packet: DimensionPacket = await queue.get()
                await self._process_packet(packet)
                queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Worker-{dimension_id}: {str(e)}", exc_info=True)

        logger.info(f"Dimension Worker-{dimension_id} stopped.")

    async def _process_packet(self, packet: DimensionPacket):
        """
        处理单维度数据包并尝试执行多路对齐。
        """
        async with self._lock:
            key = packet.key
            dim_id = packet.dimension_id

            if key not in self.alignment_buffer:
                self.alignment_buffer[key] = {}
                # 为新 key 启动异步对齐超时任务，避免无限期等待
                self.timeout_tasks[key] = asyncio.create_task(
                    self._handle_timeout(key, self.alignment_timeout)
                )

            # 存入对应维度的缓冲区
            self.alignment_buffer[key][dim_id] = packet

            # 检查是否所有 10 个维度的数据都已到达
            if len(self.alignment_buffer[key]) == self.num_dimensions:
                # 取消该 key 的超时任务
                if key in self.timeout_tasks:
                    self.timeout_tasks[key].cancel()
                    del self.timeout_tasks[key]

                # 提取对齐帧数据
                aligned_frame = self.alignment_buffer.pop(key)
                await self.output_queue.put((key, aligned_frame))
                logger.debug(f"Successfully aligned frame for key: {key}")

    async def _handle_timeout(self, key: str, timeout: float):
        """
        处理数据对齐超时。如果在指定时间内 10 个维度未能全部到达，
        将部分对齐的数据输出或丢弃，避免缓冲区无限增长。
        """
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
                    # 放入输出队列（带有部分对齐标识）
                    await self.output_queue.put((key, partial_frame))
        except asyncio.CancelledError:
            # 正常对齐被取消，无需处理
            pass

    async def start(self):
        """
        启动路由与对齐引擎，激活 10 个维度的并行消费者。
        """
        if self.is_running:
            return
        
        self.is_running = True
        self.workers = [
            asyncio.create_task(self._dimension_worker(i))
            for i in range(self.num_dimensions)
        ]
        logger.info("MatrixRouter engine initialized and running with 10 channels.")

    async def stop(self):
        """
        停止引擎并释放所有协程资源。
        """
        if not self.is_running:
            return
        
        self.is_running = False
        # 取消所有 Worker
        for worker in self.workers:
            worker.cancel()
        
        # 等待所有 Worker 结束
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        # 取消并清理所有挂起的超时任务
        for task in self.timeout_tasks.values():
            task.cancel()
        await asyncio.gather(*self.timeout_tasks.values(), return_exceptions=True)
        self.timeout_tasks.clear()
        
        logger.info("MatrixRouter engine shutdown complete.")

    async def get_aligned_frame(self) -> Optional[tuple]:
        """
        从输出队列中获取下一个已对齐（或超时部分对齐）的数据帧。
        """
        return await self.output_queue.get()
