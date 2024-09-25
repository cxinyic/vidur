from vidur.config import SimulationConfig
from vidur.simulator import Simulator
from vidur.utils.random import set_seeds
from vidur.types import UpgradeType


from typing import Tuple


def upgrade_configs() -> Tuple[SimulationConfig, SimulationConfig]:
    config_old: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config_old.seed)

    # TODO: change this from hardcoded to CLI args
    # create a new config with changed parallelism plan
    config_new: SimulationConfig = SimulationConfig.create_from_cli_args()
    config_new.cluster_config.num_replicas = 4
    config_new.cluster_config.replica_config.tensor_parallel_size = 1
    config_new.cluster_config.replica_config.num_pipeline_stages = 1

    return config_old, config_new


# UPGRADE_WAIT_ALL: wait all the current running batches to finish
# UPGRADE_NO_WAIT: stop all the ongoing batches and start upgrade immediately
# UPGRADE_WAIT_PARTIAL: save the current generated tokens and start upgrade
# immediately


def upgrade_baseline(upgrade_type: UpgradeType) -> None:
    config_old, config_new = upgrade_configs()
    simulator_old = Simulator(config_old)
    simulator_old.run_before_upgrade(upgrade_type)
    print("Done with the first simulator run, time is ", simulator_old._time)

    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_old._time)
    simulator_new._remaining_requests = simulator_old._remaining_requests
    simulator_new._add_remaining_requests()
    simulator_new.run_after_upgrade()


def normal_run() -> None:
    config: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config.seed)

    simulator = Simulator(config)
    simulator.run()


def main() -> None:
    # upgrade_baseline(UpgradeType.UPGRADE_WAIT_ALL)
    # upgrade_baseline(UpgradeType.UPGRADE_NO_WAIT)
    upgrade_baseline(UpgradeType.UPGRADE_WAIT_PARTIAL)
    # normal_run()


if __name__ == "__main__":
    main()
