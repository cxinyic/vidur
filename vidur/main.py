from vidur.config import SimulationConfig
from vidur.config.upgrade_policy_config import UpgradeConfig
from vidur.simulator import Simulator
from vidur.utils.random import set_seeds
from vidur.types import PreUpgradeType, DuringUpgradeType, PostUpgradeType
from vidur.metrics.constants import RequestCompletionMetricsTimeSeries


from typing import Tuple


def upgrade_configs() -> Tuple[SimulationConfig, SimulationConfig]:
    config_old: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config_old.seed)

    # TODO(cxinyic): change this from hardcoded to CLI args
    # create a new config with changed parallelism plan
    config_new: SimulationConfig = SimulationConfig.create_from_cli_args()
    config_new.cluster_config.num_replicas = 4
    config_new.cluster_config.replica_config.tensor_parallel_size = 2
    config_new.cluster_config.replica_config.num_pipeline_stages = 2

    return config_old, config_new


def upgrade_serving() -> None:
    upgrade_config = UpgradeConfig(
        pre_upgrade_type=PreUpgradeType.PRE_KICK_ALL,
        during_upgrade_type=DuringUpgradeType.DURING_SERVE_NEW_PREFILL,
        post_upgrade_type=PostUpgradeType.POST_PREFILLED_FIRST,
    )
    config_old, config_new = upgrade_configs()
    config_old.upgrade_config = upgrade_config
    config_new.upgrade_config = upgrade_config
    simulator_old = Simulator(config_old)
    simulator_old.run_pre_upgrade()

    # use the old config to serve but with a memory threshold
    config_upgrade: SimulationConfig = SimulationConfig.create_from_cli_args()
    config_upgrade.cluster_config.replica_config.device_config.total_memory_gb *= 0.75
    config_upgrade.upgrade_config = upgrade_config
    simulator_upgrade = Simulator(config_upgrade)
    simulator_upgrade._set_time(simulator_old._time)
    simulator_upgrade._remaining_requests = simulator_old._remaining_requests
    simulator_upgrade._all_requests = simulator_upgrade._remaining_requests
    simulator_upgrade._add_remaining_requests(PostUpgradeType.POST_NEW_FIRST)
    for x, y in simulator_old._metric_store._request_completion_metrics_time_series[
        RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
    ]._data_series:
        simulator_upgrade._metric_store._request_completion_metrics_time_series[
            RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
        ].put(x, y)
    simulator_upgrade.run_during_upgrade_new_prefill()

    # Finish upgrading and continue serving with new config
    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_upgrade._time)
    simulator_new._remaining_requests = simulator_upgrade._remaining_requests
    simulator_new._add_remaining_requests(PostUpgradeType.POST_NEW_FIRST)

    for x, y in simulator_upgrade._metric_store._request_completion_metrics_time_series[
        RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
    ]._data_series:
        simulator_new._metric_store._request_completion_metrics_time_series[
            RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
        ].put(x, y)

    simulator_new.run_post_upgrade()


def upgrade_wait_serving() -> None:
    upgrade_config = UpgradeConfig(
        pre_upgrade_type=PreUpgradeType.PRE_WAIT_MEMORY_THRESHOLD,
        during_upgrade_type=DuringUpgradeType.DURING_SERVE_CURRENT_DECODE,
        post_upgrade_type=PostUpgradeType.POST_PREFILLED_FIRST,
    )
    config_old, config_new = upgrade_configs()
    config_old.upgrade_config = upgrade_config
    config_new.upgrade_config = upgrade_config
    simulator_old = Simulator(config_old)
    simulator_old.run_pre_during_upgrade()

    # Finish upgrading and continue serving with new config
    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_old._time)
    simulator_new._remaining_requests = simulator_old._remaining_requests
    simulator_new._add_remaining_requests(PostUpgradeType.POST_TO_FINISH_FIRST)

    for x, y in simulator_new._metric_store._request_completion_metrics_time_series[
        RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
    ]._data_series:
        simulator_new._metric_store._request_completion_metrics_time_series[
            RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
        ].put(x, y)

    simulator_new.run_post_upgrade()


def normal_run() -> None:
    config: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config.seed)
    upgrade_config = UpgradeConfig(
        pre_upgrade_type=PreUpgradeType.PRE_NOT_SET,
        during_upgrade_type=DuringUpgradeType.DURING_NOT_SET,
        post_upgrade_type=PostUpgradeType.POST_NOT_SET,
    )
    config.upgrade_config = upgrade_config

    simulator = Simulator(config)
    simulator.run()


def upgrade_run() -> None:
    upgrade_config = UpgradeConfig(
        pre_upgrade_type=PreUpgradeType.PRE_KICK_ALL,
        during_upgrade_type=DuringUpgradeType.DURING_NO_SERVING,
        post_upgrade_type=PostUpgradeType.POST_ARRIVAL_ORDER,
    )
    config_old, config_new = upgrade_configs()
    config_old.upgrade_config = upgrade_config
    config_new.upgrade_config = upgrade_config
    simulator_old = Simulator(config_old)
    simulator_old.run_pre_upgrade()
    simulator_old.run_during_upgrade_no_serving()

    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_old._time)
    simulator_new._remaining_requests = simulator_old._remaining_requests
    simulator_new._add_remaining_requests(PostUpgradeType.POST_NEW_FIRST)

    for x, y in simulator_old._metric_store._request_completion_metrics_time_series[
        RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
    ]._data_series:
        simulator_new._metric_store._request_completion_metrics_time_series[
            RequestCompletionMetricsTimeSeries.REQUEST_COMPLETION
        ].put(x, y)
    simulator_new.run_post_upgrade()


def main() -> None:
    normal_run()
    # upgrade_run()
    # upgrade_serving()
    # upgrade_wait_serving()


if __name__ == "__main__":
    main()
