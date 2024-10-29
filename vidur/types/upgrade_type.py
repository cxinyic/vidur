from vidur.types.base_int_enum import BaseIntEnum

class PreUpgradeType(BaseIntEnum):
    # Different ways to handle existing requests before starting the upgrade
    # NOT_SET: upgrade type not specified
    # KICK_ALL: kick all current requests immediately
    # WAIT_MEMORY_THRESHOLD: continue current requests until memory below threshold C
    # KICK_TO_MEMORY_THRESHOLD: kick requests until memory below threshold C
    # WAIT_ALL: continue all current requests until they finish
    
    PRE_NOT_SET = 0
    PRE_KICK_ALL = 1
    PRE_WAIT_MEMORY_THRESHOLD = 2
    PRE_KICK_TO_MEMORY_THRESHOLD = 3
    PRE_WAIT_ALL = 4


class DuringUpgradeType(BaseIntEnum):
    # Different ways to handle serving during the upgrade process
    # NO_SERVING: stop all serving during upgrade
    # SERVE_NEW_PREFILL: serve new prefill requests while upgrading
    # SERVE_CURRENT_DECODE: continue decoding current requests while upgrading

    DURING_NOT_SET = 0
    DURING_NO_SERVING = 1
    DURING_SERVE_NEW_PREFILL = 2
    DURING_SERVE_CURRENT_DECODE = 3


class PostUpgradeType(BaseIntEnum):
    # Different ways to reschedule requests after upgrade is complete
    # ARRIVAL_ORDER: schedule based on original arrival time
    # PREFILLED_FIRST: prioritize requests that were already prefilled
    # NEW_FIRST: prioritize requests that haven't been prefilled

    POST_NOT_SET = 0
    POST_ARRIVAL_ORDER = 1
    POST_PREFILLED_FIRST = 2
    POST_NEW_FIRST = 3
    POST_TO_FINISH_FIRST = 4
