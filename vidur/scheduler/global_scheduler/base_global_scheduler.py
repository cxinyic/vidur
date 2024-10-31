from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from vidur.config import SimulationConfig
from vidur.entities import Replica, Request
from vidur.execution_time_predictor import ExecutionTimePredictorRegistry
from vidur.scheduler.replica_scheduler.replica_scheduler_registry import (
    ReplicaSchedulerRegistry,
)
import copy


def find_power_of_two_bounds(a: int) -> tuple[int, int, float]:
    """
    Find the closest powers of 2 that bound the given number a.
    Returns (lower_bound, upper_bound, weight_upper) where:
    - lower_bound is the largest power of 2 that's <= a
    - upper_bound is the smallest power of 2 that's >= a
    - weight_upper is the interpolation weight for upper bound (0 to 1)

    Examples:
        6 -> (4, 8, 0.5)  # halfway between 4 and 8
        4 -> (4, 4, 0.0)  # exactly at lower bound
        5 -> (4, 8, 0.25) # 25% from 4 to 8
        16 -> (16, 16, 0.0) # exactly power of 2
        17 -> (16, 32, 0.0625) # 6.25% from 16 to 32
    """

    if a <= 0:
        return (0, 1, 0.0)
    if a == 1:
        return (1, 1, 0.0)

    lower = 1
    while lower * 2 <= a:
        lower *= 2

    upper = lower
    if upper < a:
        upper *= 2

        total_distance = upper - lower
        distance_from_lower = a - lower
        weight_upper = distance_from_lower / total_distance
    else:
        weight_upper = 0.0

    return (lower, upper, weight_upper)


class BaseGlobalScheduler(ABC):
    def __init__(self, config: SimulationConfig, replicas: Dict[int, Replica]):
        self._config = config
        self._replicas = replicas

        self._num_replicas = len(self._replicas)

        tp_lower_bound, tp_upper_bound, weight = find_power_of_two_bounds(
            config.cluster_config.replica_config.tensor_parallel_size
        )

        replica_config_extra1 = copy.deepcopy(config.cluster_config.replica_config)
        replica_config_extra1.tensor_parallel_size = tp_lower_bound
        execution_time_predictor = ExecutionTimePredictorRegistry.get(
            config.execution_time_predictor_config.get_type(),
            predictor_config=config.execution_time_predictor_config,
            replica_config=replica_config_extra1,
            replica_scheduler_config=config.cluster_config.replica_scheduler_config,
            metrics_config=config.metrics_config,
        )
        replica_config_extra2 = copy.deepcopy(config.cluster_config.replica_config)
        replica_config_extra2.tensor_parallel_size = tp_upper_bound
        execution_time_predictor_extra = ExecutionTimePredictorRegistry.get(
            config.execution_time_predictor_config.get_type(),
            predictor_config=config.execution_time_predictor_config,
            replica_config=replica_config_extra2,
            replica_scheduler_config=config.cluster_config.replica_scheduler_config,
            metrics_config=config.metrics_config,
        )
        self._replica_schedulers = {
            replica_id: ReplicaSchedulerRegistry.get(
                config.cluster_config.replica_scheduler_config.get_type(),
                replica_config=config.cluster_config.replica_config,
                replica_scheduler_config=config.cluster_config.replica_scheduler_config,
                request_generator_config=config.request_generator_config,
                replica=replica,
                num_stages=replica.num_pipeline_stages,
                execution_time_predictor=execution_time_predictor,
                execution_time_predictor_extra=execution_time_predictor_extra,
                tensor_parallel_weight=weight,
            )
            for replica_id, replica in replicas.items()
        }
        self._request_queue = []

    def sort_requests(self) -> None:
        self._request_queue.sort(key=lambda request: request._arrived_at)

    def add_request(self, request: Request) -> None:
        self._request_queue.append(request)

    def get_replica_scheduler(self, replica_id: int):
        return self._replica_schedulers[replica_id]

    def get_replica_stage_scheduler(self, replica_id: int, stage_id: int):
        return self._replica_schedulers[replica_id].get_replica_stage_scheduler(
            stage_id
        )

    def is_empty(self) -> bool:
        return len(self._request_queue) == 0 and all(
            replica_scheduler.is_empty()
            for replica_scheduler in self._replica_schedulers.values()
        )

    @abstractmethod
    def schedule(self) -> List[Tuple[int, Request]]:
        pass
