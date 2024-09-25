from vidur.types.base_int_enum import BaseIntEnum


class UpgradeType(BaseIntEnum):
    # different ways to handle upgrades
    # NO_WAIT: stop all the current events and upgrade
    # WAIT_PARTIAL: wait for the current decode iteration and then upgrade
    # WAIT_ALL: wait for all the scheduled batches to finish and then upgrade
    # SERVE_XXX: overlap the upgrade with serving
    # SERVE_KICK_ALL: kick out all the ongoing batches
    # SERVE_WAIT_PARTIAL: wait for the used memory size below threshold

    UPGRADE_NOT_SET = 0
    UPGRADE_NO_WAIT = 1
    UPGRADE_WAIT_PARTIAL = 2
    UPGRADE_WAIT_ALL = 3
    UPGRADE_SERVE_KICK_ALL = 4
    UPGRADE_SERVE_WAIT_PARTIAL = 5
