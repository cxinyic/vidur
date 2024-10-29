from typing import List

from vidur.entities import Batch
from vidur.events import BaseEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.scheduler import BaseGlobalScheduler
from vidur.types import EventType, PreUpgradeType

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
        from vidur.events.upgrade_pre_finish_event import UpgradePreFinishEvent

        self._batch.on_batch_end(self.time)
        replica_scheduler = scheduler.get_replica_scheduler(self._replica_id)
        replica_scheduler.on_batch_end(self._batch)

        memory_usage_percent = replica_scheduler.memory_usage_percent
        metrics_store.on_batch_end(
            self.time, self._batch, self._replica_id, memory_usage_percent
        )
        if (
            self._pre_upgrade_flag == PreUpgradeType.PRE_WAIT_MEMORY_THRESHOLD
            or self._pre_upgrade_flag == PreUpgradeType.PRE_KICK_TO_MEMORY_THRESHOLD
        ) and not replica_scheduler._pre_upgrade_finish:
            # TODO(cxinyic): change this from hardcode to the config
            # logger.info(
            #     "Upgrade serve wait partial, memory usage: %f", memory_usage_percent
            # )
            if memory_usage_percent < 75:
                replica_scheduler._pre_upgrade_finish = True
                return [
                    UpgradePreFinishEvent(self.time),
                    ReplicaScheduleEvent(self.time, self._replica_id),
                ]

        return [ReplicaScheduleEvent(self.time, self._replica_id)]

    def to_dict(self):
        return {
            "time": self.time,
            "event_type": self.event_type,
            "batch_id": self._batch.id,
        }
