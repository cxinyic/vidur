import atexit
import heapq
import json
from typing import List

from vidur.config import SimulationConfig
from vidur.entities import Cluster
from vidur.events import BaseEvent, RequestArrivalEvent
from vidur.events.upgrade_event import UpgradeEvent
from vidur.events.upgrade_finish_event import UpgradeFinishEvent
from vidur.logger import init_logger
from vidur.metrics import MetricsStore
from vidur.request_generator import RequestGeneratorRegistry
from vidur.scheduler import BaseGlobalScheduler, GlobalSchedulerRegistry
from vidur.types import EventType, UpgradeType

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
        self._upgrade_time = 15
        self._remaining_requests = []
        self._num_upgrade_real_start = 0
        self._all_requests = []

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
        self.run_after_upgrade()

    def run_after_upgrade(self) -> None:
        logger.info(
            f"Starting simulation with cluster: {self._cluster} and {len(self._event_queue)} requests"
        )
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
        logger.info(f"Simulation after upgrade ended at: {self._time}s")

    # overlap serving with upgrade
    def run_with_upgrade(self) -> None:
        # manually set upgrade finishing time(current time + upgrade time)
        self._init_upgrade_finish_event()

        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)
            #
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

    def run_before_upgrade(self, upgrade_type: UpgradeType) -> None:
        self._init_event_queue()
        self._init_upgrade_event()
        logger.info(
            f"Starting simulation with cluster: {self._cluster} and {len(self._event_queue)} requests"
        )

        while self._event_queue and not self._terminate:
            _, event = heapq.heappop(self._event_queue)
            self._set_time(event._time)
            # For upgrade event, set global upgrade flag
            if event._event_type == EventType.UPGRADE:
                logger.info(f"Upgrade event triggered at time: {event._time}")
                self._global_upgrade_flag = True

            # For serve_wait_partial upgrade, if memory usage for all replicas
            # is low enough, break the loop to do real upgrade
            if event._event_type == EventType.UPGRADE_REAL_START:
                self._num_upgrade_real_start += 1
                if self._num_upgrade_real_start == len(
                    self._scheduler._replica_schedulers
                ):
                    break
            
            # global_upgrade_flag means that we have met the upgrade event.
            # For upgrade_no_wait/wait_partial/serve_kick_all, break the loop and upgrade immediately
            # For upgrade_wait_all, continue the loop until all the scheduled batches are finished
            if self._global_upgrade_flag:
                if (
                    upgrade_type == UpgradeType.UPGRADE_NO_WAIT
                    or upgrade_type == UpgradeType.UPGRADE_WAIT_PARTIAL
                    or upgrade_type == UpgradeType.UPGRADE_SERVE_KICK_ALL
                ):
                    break
                else:
                    event.set_upgrade_flag(upgrade_type)

            # logger.info(f"event: {event._event_type} at time: {event._time}, upgrade flag: {event._upgrade_flag}")
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
        if (
            upgrade_type != UpgradeType.UPGRADE_SERVE_KICK_ALL
            and upgrade_type != UpgradeType.UPGRADE_SERVE_WAIT_PARTIAL
        ):
            self._set_time(self._time + self._upgrade_time)

        # record the unfinished requests
        if upgrade_type == UpgradeType.UPGRADE_NO_WAIT:
            # For those scheduled batches that are not finished, reschedule them
            for replica_id in self._scheduler._replica_schedulers:
                replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
                for request in replica_scheduler._unfinished_request_queue.values():
                    if request not in replica_scheduler._request_queue:
                        request.reschedule()
                self._remaining_requests.extend(
                    list(replica_scheduler._unfinished_request_queue.values())
                )
        elif (
            upgrade_type == UpgradeType.UPGRADE_WAIT_PARTIAL
            or upgrade_type == UpgradeType.UPGRADE_SERVE_KICK_ALL
            or upgrade_type == UpgradeType.UPGRADE_SERVE_WAIT_PARTIAL
        ):
            # For those scheduled batches that are not finished, partially reschedule them
            for replica_id in self._scheduler._replica_schedulers:
                replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
                for request in replica_scheduler._unfinished_request_queue.values():
                    if request not in replica_scheduler._request_queue:
                        request.reschedule_partial()
                self._remaining_requests.extend(
                    list(replica_scheduler._unfinished_request_queue.values())
                )
        else:
            # The scheduled batches are finished, only consider request_queue
            for replica_id in self._scheduler._replica_schedulers:
                replica_scheduler = self._scheduler.get_replica_scheduler(replica_id)
                self._remaining_requests.extend(replica_scheduler._request_queue)

        for request in self._all_requests:
            if request not in self._remaining_requests:
                self._remaining_requests.append(request)

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

    def _init_upgrade_event(self) -> None:
        # TODO: check upgrade event time
        logger.info(f"Add upgrade event at time 15")
        self._add_event(UpgradeEvent(15))

    def _init_upgrade_finish_event(self) -> None:
        self._add_event(UpgradeFinishEvent(self._time + self._upgrade_time))

    def _add_remaining_requests(self) -> None:
        for request in self._remaining_requests:
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
