from dataclasses import dataclass
from qlir.servers.logging.logging_profiles import LogProfile


@dataclass(frozen=True)
class RuntimeConfig:
    log_profile: LogProfile = LogProfile.QLIR_INFO