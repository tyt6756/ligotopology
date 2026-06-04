import asyncio
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
    """
    维度数据包，用于承载单通道输入的异步多维数据。
    """
    def __init__(self, dimension_id: int, key: str, payload: Any, timestamp: float = None):
        self.dimension_id = dimension_id
        self.key = key
        self.payload = payload
        self.timestamp = timestamp or time.time()

    def __repr__(self) -> str:
        return f"DimensionPacket(dim={self.dimension_id}, key={self.key}, ts={self.timestamp})"


class MatrixRouter:
    """
    LigoTopology 多通道异步数据路由与对齐引擎。
    """
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
