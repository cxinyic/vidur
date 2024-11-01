from math import ceil
from typing import List

from vidur.entities.batch import Batch, Request
from vidur.logger import init_logger
from vidur.scheduler.replica_scheduler.base_replica_scheduler import (
    BaseReplicaScheduler,
)

logger = init_logger(__name__)


class VLLMReplicaScheduler(BaseReplicaScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._preempted_requests: List[Request] = []
        self._num_running_batches = 0
        # For vLLM and its derivatives, we only need to set a loose max batch size
        # Memory requirements are handled explicitly by the scheduler
        self._max_micro_batch_size = self._config.batch_size_cap // self._num_stages
        self._watermark_blocks = int(
            self._config.watermark_blocks_fraction * self._config.num_blocks
        )

        self._kick_flag = False

    def on_batch_end(self, batch: Batch) -> None:
        self._num_running_batches -= 1

        for request in batch.requests:
            if request.completed:
                self.free(request.id)
            else:
                self._preempted_requests.append(request)

    def _can_allocate_request(self, request: Request) -> bool:
        if request.id not in self._allocation_map:
            # new request
            num_required_blocks = ceil(
                (request.num_prefill_tokens) / self._config.block_size
            )
            return (
                self._config.num_blocks
                - self._num_allocated_blocks
                - num_required_blocks
                >= self._watermark_blocks
            )

        # vllm requires at least one block to be available
        return self._config.num_blocks - self._num_allocated_blocks >= 1

    # TODO(cxinyic): deal with MemoryPlanner rather than directly hardcode the num_blocks
    def _can_allocate_request_after_kick(self, request: Request) -> bool:
        if request.id not in self._allocation_map:
            # new request
            num_required_blocks = ceil(
                (request.num_prefill_tokens) / self._config.block_size
            )
            return (
                self._config.num_blocks / 2
                - self._num_allocated_blocks
                - num_required_blocks
                >= self._watermark_blocks
            )

        # vllm requires at least one block to be available
        return self._config.num_blocks / 2 - self._num_allocated_blocks >= 1

    def _allocate_request(self, request: Request) -> None:
        if request.id not in self._allocation_map:
            # new request
            num_required_blocks = ceil(
                (request.num_prefill_tokens) / self._config.block_size
            )
            self.allocate(request.id, num_required_blocks)
            return

        num_tokens_reserved = self._allocation_map[request.id] * self._config.block_size
        num_tokens_required = max(0, request.num_processed_tokens - num_tokens_reserved)
        assert (
            num_tokens_required == 0 or num_tokens_required == 1
        ), f"num_tokens_required: {num_tokens_required}"

        if num_tokens_required == 0:
            return

        self.allocate(request.id, 1)

  
    def _get_next_batch(self) -> Batch:
        requests = []
        num_tokens = []
        num_batch_tokens = 0

        while self._request_queue:
            request = self._request_queue[0]

            next_num_tokens = self._get_request_next_num_tokens(request)

            if not self._can_allocate_request(request):
                break

            new_num_tokens = num_tokens + [next_num_tokens]
            new_num_batch_tokens = len(new_num_tokens) * max(new_num_tokens)
            if new_num_batch_tokens > self._config.max_tokens_in_batch:
                break

            if len(self._allocation_map) == self._config.batch_size_cap:
                break

            if len(requests) == self._max_micro_batch_size:
                break

            request = self._request_queue.pop(0)

            self._allocate_request(request)
            requests.append(request)
            num_tokens.append(next_num_tokens)
            num_batch_tokens += next_num_tokens

        if requests:
            return Batch(self._replica_id, requests, num_tokens)

        # Safer to sort preempted_requests to maintain FIFO order
        self._preempted_requests.sort(key=lambda r: r.arrived_at)
        # all preempted_requests will have prefill completed
        while self._preempted_requests:
            if len(requests) == self._max_micro_batch_size:
                break

            request = self._preempted_requests.pop(0)

            while not self._can_allocate_request(request):
                if self._preempted_requests:
                    victim_request = self._preempted_requests.pop(-1)
                    victim_request.restart()
                    self.free(victim_request.id)
                    self._request_queue = [victim_request] + self._request_queue
                    self._unfinished_request_queue[victim_request.id] = victim_request
                else:
                    request.restart()
                    self.free(request.id)
                    self._request_queue = [request] + self._request_queue
                    self._unfinished_request_queue[request.id] = request
                    break
            else:
                self._allocate_request(request)
                next_num_tokens = self._get_request_next_num_tokens(request)
                requests.append(request)
                num_tokens.append(next_num_tokens)

        if not requests:
            return

        return Batch(self._replica_id, requests, num_tokens)

    # prioritize decoding phase
    def _get_next_batch_decode(self) -> Batch:
        # logger.info(f"running get_next_batch_upgrade,id: {self.replica_id}, with pending requests: {self.num_pending_requests}")
        requests = []
        num_tokens = []

        # Safer to sort preempted_requests to maintain FIFO order
        self._preempted_requests.sort(key=lambda r: r.arrived_at)
        # all preempted_requests will have prefill completed
        while self._preempted_requests:
            if len(requests) == self._max_micro_batch_size:
                break

            request = self._preempted_requests.pop(0)

            while not self._can_allocate_request(request):
                if self._preempted_requests:
                    victim_request = self._preempted_requests.pop(-1)
                    victim_request.restart()
                    self.free(victim_request.id)
                    self._request_queue = [victim_request] + self._request_queue
                    self._unfinished_request_queue[victim_request.id] = victim_request
                else:
                    request.restart()
                    self.free(request.id)
                    self._request_queue = [request] + self._request_queue
                    self._unfinished_request_queue[request.id] = request
                    break
            else:
                self._allocate_request(request)
                next_num_tokens = self._get_request_next_num_tokens(request)
                requests.append(request)
                num_tokens.append(next_num_tokens)

        if not requests:
            return
        logger.info(
            "replica id %d, current memory usage: %f",
            self.replica_id,
            self.memory_usage_percent,
        )
        return Batch(self._replica_id, requests, num_tokens)

    def _get_next_batch_kick_to_memory_threshold(self) -> Batch:
        requests = []
        num_tokens = []
        

        # TODO(cxinyic): change this from hardcode to the config
        # TODO(cxinyic): based on time order or to finish order to kick requests
        self._preempted_requests.sort(key=lambda r: r.arrived_at,
        reverse=True)
        if self.memory_usage_percent >= 50:
            logger.info(
                "before kick, replica id %d, current memory usage: %f",
                self.replica_id,
                self.memory_usage_percent,
            )
            while self.memory_usage_percent >= 50:
                request = self._preempted_requests.pop(0)
                request.restart()
                self.free(request.id)
                self._request_queue = [request] + self._request_queue
                self._unfinished_request_queue[request.id] = request
            self._kick_flag = True
        if self._kick_flag:
            logger.info(
                "after kick, replica id %d, current memory usage: %f",
                self.replica_id,
                self.memory_usage_percent,
            )
            self._kick_flag = False

        # logger.info("current memory usage: %f", self.memory_usage_percent)
        # Safer to sort preempted_requests to maintain FIFO order
        self._preempted_requests.sort(key=lambda r: r.arrived_at)
        # all preempted_requests will have prefill completed
        while self._preempted_requests:
            if len(requests) == self._max_micro_batch_size:
                break

            request = self._preempted_requests.pop(0)

            while not self._can_allocate_request_after_kick(request):
                if self._preempted_requests:
                    victim_request = self._preempted_requests.pop(-1)
                    victim_request.restart()
                    self.free(victim_request.id)
                    self._request_queue = [victim_request] + self._request_queue
                    self._unfinished_request_queue[victim_request.id] = victim_request
                else:
                    request.restart()
                    self.free(request.id)
                    self._request_queue = [request] + self._request_queue
                    self._unfinished_request_queue[request.id] = request
                    break
            else:
                self._can_allocate_request_after_kick(request)
                next_num_tokens = self._get_request_next_num_tokens(request)
                requests.append(request)
                num_tokens.append(next_num_tokens)

        if not requests:
            return
        logger.info(
            "replica id %d, current memory usage: %f",
            self.replica_id,
            self.memory_usage_percent,
        )
        return Batch(self._replica_id, requests, num_tokens)
    
