from typing import List

from vidur.entities import Request
from vidur.events.base_event import BaseEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.scheduler import BaseGlobalScheduler
from vidur.types import EventType

logger = init_logger(__name__)


class UpgradeRealStartEvent(BaseEvent):
    def __init__(self, time: float, replica_id: int) -> None:
        super().__init__(time, EventType.UPGRADE_REAL_START)
        self._replica_id = replica_id
    
    def handle_event(
        self, scheduler: BaseGlobalScheduler, metrics_store: MetricsStore
    ) -> List[BaseEvent]:
        return []

    def to_dict(self) -> dict:
        return {
            "time": self.time,
            "event_type": self.event_type,
        }
