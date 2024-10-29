import atexit
import heapq
import json
from typing import List

from vidur.config import SimulationConfig
from vidur.entities import Cluster
from vidur.events import BaseEvent, RequestArrivalEvent
from vidur.events.upgrade_trigger_event import UpgradeTriggerEvent
from vidur.events.upgrade_finish_event import UpgradeFinishEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.request_generator import RequestGeneratorRegistry
from vidur.scheduler import BaseGlobalScheduler, GlobalSchedulerRegistry
from vidur.types import (
    EventType,
    PreUpgradeType,
    DuringUpgradeType,
    PostUpgradeType,
)

logger = init_logger(__name__)


class Simulator:
    def __init__(self, config: SimulationConfig) -> None:
        self._config: SimulationConfig = config

        self._time = 0
        self._terminate = False
        self._time_limit = self._config.time_limit
        if not self._time_limit:
            self._time_limit = float("inf")

        self._event_queue = []

        self._event_trace = []
        self._event_chrome_trace = []

        self._cluster = Cluster(
            self._config.cluster_config,
            self._config.metrics_config,
            self._config.request_generator_config,
        )
        self._metric_store = MetricsStore(self._config)
        self._request_generator = RequestGeneratorRegistry.get(
            self._config.request_generator_config.get_type(),
            self._config.request_generator_config,
        )
        self._scheduler = GlobalSchedulerRegistry.get(
            self._config.cluster_config.global_scheduler_config.get_type(),
            self._config,
            self._cluster.replicas,
        )
        logger.info(f"Cluster: {self._cluster.to_dict()}")

        # upgrade related
        self._global_upgrade_flag = False
        self._upgrade_time = 30
        self._remaining_requests = []
        self._num_upgrade_pre_finish = 0
        self._all_requests = []

        self._print_num = 0

        atexit.register(self._write_output)

    @property
    def scheduler(self) -> BaseGlobalScheduler:
        return self._scheduler

    @property
    def metric_store(self) -> MetricsStore:
        return self._metric_store

    # normal run without upgrade event
    def run(self) -> None:
        self._init_event_queue()
        self.run_post_upgrade()

    # For PRE_KICK_ALL or PRE_WAIT_ALL
    def run_pre_upgrade(self) -> None:
        self._init_event_queue()
        self._init_upgrade_trigger_event()

        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)
            # Upgrade signal is received, trigger the upgrade process
            if event._event_type == EventType.UPGRADE_TRIGGER:
                # for kick_all policy, break the loop and upgrade immediately
                if (
                    self._config.upgrade_config.pre_upgrade_type
                    == PreUpgradeType.PRE_KICK_ALL
                ):
                    break
                else:
                    self._global_upgrade_flag = True

            # For PRE_WAIT_ALL policy, continue the loop but set upgrade flag for events
            if self._global_upgrade_flag:
                event.set_pre_upgrade_flag(self._config.upgrade_config.pre_upgrade_type)

            new_events = event.handle_event(self._scheduler, self._metric_store)

            # remove the global scheduled requests from all_requests
            if event._event_type == EventType.GLOBAL_SCHEDULE:
                for _, request in event._request_mapping:
                    self._all_requests.remove(request)

            self._add_events(new_events)

            if self._config.metrics_config.write_json_trace:
                self._event_trace.append(event.to_dict())

            if self._config.metrics_config.enable_chrome_trace:
                chrome_trace = event.to_chrome_trace()
                if chrome_trace:
                    self._event_chrome_trace.append(chrome_trace)

        logger.info(f"Simulation before upgrade ended at: {self._time}s")

        # TODO(cxinyic): check whether these two can share the same code

        # PRE_WAIT_ALL: The scheduled batches are finished, only consider request_queue
        if self._config.upgrade_config.pre_upgrade_type == PreUpgradeType.PRE_WAIT_ALL:
            for replica_id in self._scheduler._replica_schedulers:
                replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
                self._remaining_requests.extend(replica_scheduler._request_queue)
        # PRE_KICK_ALL: for scheduled batches that are not finished, reschedule each request
        else:
            for replica_id in self._scheduler._replica_schedulers:
                replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
                for request in replica_scheduler._unfinished_request_queue.values():
                    if request not in replica_scheduler._request_queue:
                        request.reschedule_partial()
                self._remaining_requests.extend(
                    list(replica_scheduler._unfinished_request_queue.values())
                )
        # For request which has not been scheduled by global scheduler
        for request in self._all_requests:
            if request not in self._remaining_requests:
                self._remaining_requests.append(request)

    # TODO(cxinyic): For PRE_WAIT_MEMORY_THRESHOLD and PRE_KICK_MEMORY_THRESHOLD
    def run_pre_during_upgrade(self) -> None:
        self._init_event_queue()
        self._init_upgrade_trigger_event()

        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)
            # Upgrade signal is received, trigger the upgrade process
            if event._event_type == EventType.UPGRADE_TRIGGER:
                self._global_upgrade_flag = True

            # continue the loop but set upgrade flag for events
            if self._global_upgrade_flag:
                event.set_pre_upgrade_flag(self._config.upgrade_config.pre_upgrade_type)

            if event._event_type == EventType.UPGRADE_FINISH:
                break

            # For WAIT_XXX policy, waiting threshold is reached, break the loop
            if event._event_type == EventType.UPGRADE_PRE_FINISH:
                self._num_upgrade_pre_finish += 1
                if self._num_upgrade_pre_finish == len(
                    self._scheduler._replica_schedulers
                ):
                    # TODO(cxinyic): start upgrade with serving_decode (send
                    # another event)
                    self._init_upgrade_finish_event()

            new_events = event.handle_event(self._scheduler, self._metric_store)

            # remove the global scheduled requests from all_requests
            if event._event_type == EventType.GLOBAL_SCHEDULE:
                for _, request in event._request_mapping:
                    self._all_requests.remove(request)

            self._add_events(new_events)

            if self._config.metrics_config.write_json_trace:
                self._event_trace.append(event.to_dict())

            if self._config.metrics_config.enable_chrome_trace:
                chrome_trace = event.to_chrome_trace()
                if chrome_trace:
                    self._event_chrome_trace.append(chrome_trace)

        logger.info(f"Simulation before and during upgrade ended at: {self._time}s")

        for replica_id in self._scheduler._replica_schedulers:
            replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
            for request in replica_scheduler._unfinished_request_queue.values():
                if request not in replica_scheduler._request_queue:
                    request.reschedule_partial()
            self._remaining_requests.extend(
                list(replica_scheduler._unfinished_request_queue.values())
            )
        # For request which has not been scheduled by global scheduler
        for request in self._all_requests:
            if request not in self._remaining_requests:
                self._remaining_requests.append(request)

    # no serving during upgrade
    def run_during_upgrade_no_serving(self) -> None:
        self._set_time(self._time + self._upgrade_time)
        return

    # serving during upgrade with new prefills
    def run_during_upgrade_new_prefill(self) -> None:
        self._init_upgrade_finish_event()

        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)

            if event._event_type == EventType.UPGRADE_FINISH:
                break

            new_events = event.handle_event(self._scheduler, self._metric_store)
            # remove the global scheduled requests from all_requests
            if event._event_type == EventType.GLOBAL_SCHEDULE:
                for _, request in event._request_mapping:
                    self._all_requests.remove(request)

            self._add_events(new_events)

            if self._config.metrics_config.write_json_trace:
                self._event_trace.append(event.to_dict())

            if self._config.metrics_config.enable_chrome_trace:
                chrome_trace = event.to_chrome_trace()
                if chrome_trace:
                    self._event_chrome_trace.append(chrome_trace)

        logger.info(f"Simulation during upgrade ended at: {self._time}s")
        for replica_id in self._scheduler._replica_schedulers:
            replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
            for request in replica_scheduler._unfinished_request_queue.values():
                if request not in replica_scheduler._request_queue:
                    request.reschedule_partial()
            self._remaining_requests.extend(
                list(replica_scheduler._unfinished_request_queue.values())
            )

        for request in self._all_requests:
            if request not in self._remaining_requests:
                self._remaining_requests.append(request)

    def run_post_upgrade(self) -> None:
        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)

            new_events = event.handle_event(self._scheduler, self._metric_store)
            self._add_events(new_events)

            if self._config.metrics_config.write_json_trace:
                self._event_trace.append(event.to_dict())

            if self._config.metrics_config.enable_chrome_trace:
                chrome_trace = event.to_chrome_trace()
                if chrome_trace:
                    self._event_chrome_trace.append(chrome_trace)

        # assert self._scheduler.is_empty() or self._terminate
        logger.info(f"Simulation post upgrade ended at: {self._time}s")

    def _write_output(self) -> None:
        logger.info("Writing output")

        self._metric_store.plot()
        logger.info("Metrics written")

        if self._config.metrics_config.write_json_trace:
            self._write_event_trace()
            self._scheduler.write_batching_history()
            logger.info("Json event trace written")

        if self._config.metrics_config.enable_chrome_trace:
            self._write_chrome_trace()
            logger.info("Chrome event trace written")

    def _add_event(self, event: BaseEvent) -> None:
        heapq.heappush(self._event_queue, (event._priority_number, event))

    def _add_events(self, events: List[BaseEvent]) -> None:
        for event in events:
            self._add_event(event)

    def _init_event_queue(self) -> None:
        requests = self._request_generator.generate()
        self._all_requests = requests

        for request in requests:
            self._add_event(RequestArrivalEvent(request.arrived_at, request))

    def _init_upgrade_trigger_event(self) -> None:
        # upgrade signal will be sent at time 200
        self._add_event(UpgradeTriggerEvent(200))

    def _init_upgrade_finish_event(self) -> None:
        self._add_event(UpgradeFinishEvent(self._time + self._upgrade_time))

    def _add_remaining_requests(self, post_upgrade_type: PostUpgradeType) -> None:
        # prefer new requests
        if post_upgrade_type == PostUpgradeType.POST_NEW_FIRST:
            sorted_requests = sorted(
                self._remaining_requests,
                key=lambda r: r._num_processed_tokens_before_upgrade,
            )
        # prefer prefilled requests
        elif post_upgrade_type == PostUpgradeType.POST_PREFILLED_FIRST:
            sorted_requests = sorted(
                self._remaining_requests,
                key=lambda r: r._num_processed_tokens_before_upgrade,
                reverse=True,
            )
        elif post_upgrade_type == PostUpgradeType.POST_TO_FINISH_FIRST:
            sorted_requests = sorted(
                self._remaining_requests,
                key=lambda r: r.total_tokens - r._num_processed_tokens_before_upgrade,
            )
        # Default or POST_ARRIVAL_ORDER: schedule based on original arrival time
        else:
            sorted_requests = self._remaining_requests

        for request in sorted_requests:
            # logger.info(f"Adding remaining request with processed tokens: {request.id, request._num_processed_tokens_before_upgrade}")
            if request.arrived_at >= self._time:
                self._add_event(RequestArrivalEvent(request.arrived_at, request))
            else:
                self._add_event(RequestArrivalEvent(self._time, request))
        self._remaining_requests = []

    def _set_time(self, time: float) -> None:
        self._time = time
        if self._time > self._time_limit:
            logger.info(
                f"Time limit reached: {self._time_limit}s terminating the simulation."
            )
            self._terminate = True

    def _write_event_trace(self) -> None:
        trace_file = f"{self._config.metrics_config.output_dir}/event_trace.json"
        with open(trace_file, "w") as f:
            json.dump(self._event_trace, f)

    def _write_chrome_trace(self) -> None:
        trace_file = f"{self._config.metrics_config.output_dir}/chrome_trace.json"

        chrome_trace = {"traceEvents": self._event_chrome_trace}

        with open(trace_file, "w") as f:
            json.dump(chrome_trace, f)
