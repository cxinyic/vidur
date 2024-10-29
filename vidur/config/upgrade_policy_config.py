from dataclasses import dataclass, field
from typing import Optional

from vidur.types import DuringUpgradeType, PostUpgradeType, PreUpgradeType


@dataclass
class UpgradeConfig:
    pre_upgrade_type: PreUpgradeType = field(
        default=PreUpgradeType.PRE_NOT_SET,
        metadata={"help": "How to handle requests before upgrade starts."}
    )
    during_upgrade_type: DuringUpgradeType = field(
        default=DuringUpgradeType.DURING_NOT_SET,
        metadata={"help": "How to handle serving during upgrade process."}
    )
    post_upgrade_type: PostUpgradeType = field(
        default=PostUpgradeType.POST_NOT_SET,
        metadata={"help": "How to schedule requests after upgrade."}
    )
    time_threshold: Optional[float] = field(
        default=10,
        metadata={"help": "Time threshold T for PRE_WAIT_TIME_THRESHOLD policy."}
    )
    memory_threshold: Optional[float] = field(
        default=50,
        metadata={"help": "Memory threshold C for PRE_WAIT_MEMORY_THRESHOLD policy."}
    )
    completion_threshold: Optional[float] = field(
        default=20,
        metadata={"help": "Completion percentage X for PRE_WAIT_COMPLETION_THRESHOLD policy."}
    )
