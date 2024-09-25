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

def upgrade_serving(upgrade_type: UpgradeType) -> None:
    # initial run with the old config and receive the upgrade event
    config_old, config_new = upgrade_configs()
    simulator_old = Simulator(config_old)
    simulator_old.run_before_upgrade(upgrade_type)
    print("Done with the first simulator run, time is ", simulator_old._time)

    # use the old config to serve but with a memory threshold
    config_upgrade: SimulationConfig = SimulationConfig.create_from_cli_args()
    config_upgrade.cluster_config.replica_config.device_config.total_memory_gb *= 0.4
    simulator_upgrade = Simulator(config_upgrade)
    simulator_upgrade._set_time(simulator_old._time)
    simulator_upgrade._remaining_requests = simulator_old._remaining_requests
    simulator_upgrade._all_requests = simulator_upgrade._remaining_requests
    simulator_upgrade._add_remaining_requests()
    simulator_upgrade.run_with_upgrade()

    # Finish upgrading and continue serving with new config
    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_old._time)
    simulator_new._remaining_requests = simulator_upgrade._remaining_requests
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
    # upgrade_baseline(UpgradeType.UPGRADE_WAIT_PARTIAL)
    # upgrade_serving(UpgradeType.UPGRADE_SERVE_WAIT_PARTIAL)
    upgrade_serving(UpgradeType.UPGRADE_SERVE_KICK_ALL)
    # normal_run()


if __name__ == "__main__":
    main()
