from typing import List

from vidur.events import BaseEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.scheduler import BaseGlobalScheduler
from vidur.types import EventType

logger = init_logger(__name__)


class ReplicaScheduleEvent(BaseEvent):
    def __init__(self, time: float, replica_id: int):
        super().__init__(time, EventType.REPLICA_SCHEDULE)

        self._replica_id = replica_id

        self._batches = []

    def handle_event(
        self, scheduler: BaseGlobalScheduler, metrics_store: MetricsStore
    ) -> List[BaseEvent]:
        from vidur.events.batch_stage_arrival_event import BatchStageArrivalEvent

        replica_scheduler = scheduler.get_replica_scheduler(self._replica_id)
        # If an upgrade event has been scheduled, do not schedule any new
        # prefilling batches, but need to keep decoding batches
        if self._upgrade_flag:
            self._batches = replica_scheduler.on_upgrade()
        else:
            self._batches = replica_scheduler.on_schedule()

        if not self._batches:
            return []

        memory_usage_percent = replica_scheduler.memory_usage_percent
        metrics_store.on_replica_schedule(
            self.time, self._replica_id, memory_usage_percent
        )

        for batch in self._batches:
            batch.on_schedule(self.time)

        return [
            BatchStageArrivalEvent(
                self.time,
                self._replica_id,
                0,  # stage_id
                batch,
            )
            for batch in self._batches
        ]

    def to_dict(self):
        return {
            "time": self.time,
            "event_type": self.event_type,
            "replica_id": self._replica_id,
            "batch_ids": [batch.id for batch in self._batches],
        }

    def to_chrome_trace(self) -> dict:
        if self._batches:
            return {
                "event_type": EventType.REPLICA_SCHEDULE,
                "ts": self.time * 1e6,
                "replica_id": self._replica_id,
                "batch": [
                    {
                        "batch_id": batch.id,
                        "request_ids": batch.request_ids,
                        "num_prefill_tokens": batch.num_prefill_tokens,
                        "num_decode_tokens": batch.num_decode_tokens,
                    }
                    for batch in self._batches
                ],
            }
        return None
