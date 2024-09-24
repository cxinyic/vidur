from vidur.config import SimulationConfig
from vidur.simulator import Simulator
from vidur.utils.random import set_seeds


from typing import Tuple


def upgrade_configs() -> Tuple[SimulationConfig, SimulationConfig]:
    config_old: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config_old.seed)

    # TODO: change this from hardcoded to CLI args
    # create a new config with changed parallelism plan
    config_new: SimulationConfig = SimulationConfig.create_from_cli_args()
    config_new.cluster_config.num_replicas = 2
    config_new.cluster_config.replica_config.tensor_parallel_size = 2

    return config_old, config_new


# upgrade baseline1: wait all the current running batches to finish
def upgrade_baseline1() -> None:
    config_old, config_new = upgrade_configs()

    simulator_old = Simulator(config_old)
    simulator_old.run_before_upgrade()
    print("Done with the first simulator run, time is ", simulator_old._time)

    simulator_new = Simulator(config_new)
    simulator_new._set_time(simulator_old._time)
    simulator_new._remaining_requests = simulator_old._remaining_requests
    simulator_new._add_remaining_requests()
    simulator_new.run_after_upgrade()


# upgrade baseline2: stop all the ongoing batches and start upgrade immediately
def upgrade_baseline2() -> None:
    config_old, config_new = upgrade_configs()
    simulator_old = Simulator(config_old)
    simulator_old.run_before_upgrade()
    print("Done with the first simulator run, time is ", simulator_old._time)


def normal_run() -> None:
    config: SimulationConfig = SimulationConfig.create_from_cli_args()
    set_seeds(config.seed)

    simulator = Simulator(config)
    simulator.run()


def main() -> None:
    upgrade_baseline1()
    # normal_run()


if __name__ == "__main__":
    main()
