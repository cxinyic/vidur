from typing import List

from vidur.entities import Batch
from vidur.events import BaseEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.scheduler import BaseGlobalScheduler
from vidur.types import EventType, UpgradeType

logger = init_logger(__name__)


class BatchEndEvent(BaseEvent):
    def __init__(self, time: float, replica_id: int, batch: Batch):
        super().__init__(time, EventType.BATCH_END)

        self._replica_id = replica_id
        self._batch = batch

    def handle_event(
        self, scheduler: BaseGlobalScheduler, metrics_store: MetricsStore
    ) -> List[BaseEvent]:
        from vidur.events.replica_schedule_event import ReplicaScheduleEvent
        from vidur.events.upgrade_real_start_event import UpgradeRealStartEvent

        self._batch.on_batch_end(self.time)
        replica_scheduler = scheduler.get_replica_scheduler(self._replica_id)
        replica_scheduler.on_batch_end(self._batch)

        memory_usage_percent = replica_scheduler.memory_usage_percent
        metrics_store.on_batch_end(
            self.time, self._batch, self._replica_id, memory_usage_percent
        )

        # for serve_wait_partial upgrade, if memory usage is already low,
        # trigger upgrade real start and still do serving(during waiting for
        # other replicas)
        if (
            self._upgrade_flag == UpgradeType.UPGRADE_SERVE_WAIT_PARTIAL
            and replica_scheduler._upgrade_real_start == False
            and memory_usage_percent < 40
        ):
            replica_scheduler._upgrade_real_start = True
            return [UpgradeRealStartEvent(self.time, self._replica_id), ReplicaScheduleEvent(self.time, self._replica_id)]

        return [ReplicaScheduleEvent(self.time, self._replica_id)]

    def to_dict(self):
        return {
            "time": self.time,
            "event_type": self.event_type,
            "batch_id": self._batch.id,
        }
