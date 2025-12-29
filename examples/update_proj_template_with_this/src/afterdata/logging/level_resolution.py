from enum import Enum
import logging

from afterdata.logging.logging_profiles import LogProfile



def resolve_levels(profile: LogProfile) -> tuple[int, int]:
    match profile:
        case LogProfile.PROD:
            return logging.WARNING, logging.WARNING

        case LogProfile.ALL_INFO:
            return logging.INFO, logging.INFO

        case LogProfile.ALL_DEBUG:
            return logging.DEBUG, logging.DEBUG

        case LogProfile.QLIR_INFO:
            return logging.WARNING, logging.INFO

        case LogProfile.QLIR_DEBUG:
            return logging.WARNING, logging.DEBUG

        case _:
            raise ValueError(f"Unknown LogProfile: {profile}")
