from vidur.types.base_int_enum import BaseIntEnum


class UpgradeType(BaseIntEnum):
    # different ways to handle upgrades
    # NO_WAIT: stop all the current events and upgrade
    # WAIT_PARTIAL: wait for the current decode iteration and then upgrade
    # WAIT_ALL: wait for all the scheduled batches to finish and then upgrade

    UPGRADE_NOT_SET = 0
    UPGRADE_NO_WAIT = 1
    UPGRADE_WAIT_PARTIAL = 2
    UPGRADE_WAIT_ALL = 3
