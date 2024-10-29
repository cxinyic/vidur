from vidur.types.base_int_enum import BaseIntEnum


class EventType(BaseIntEnum):
    # at any given time step, call the schedule event at the last
    # to ensure that all the requests are processed
    UPGRADE_TRIGGER = 1
    UPGRADE_PRE_FINISH = 2
    UPGRADE_FINISH = 3
    BATCH_STAGE_ARRIVAL = 4
    REQUEST_ARRIVAL = 5
    BATCH_STAGE_END = 6
    BATCH_END = 7
    GLOBAL_SCHEDULE = 8
    REPLICA_SCHEDULE = 9
    REPLICA_STAGE_SCHEDULE = 10

