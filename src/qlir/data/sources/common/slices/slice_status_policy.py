from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason


class SliceStatusPolicy:
    """
    Central authority for resolving SliceStatus from SliceStatusReason.

    This class encodes all legality rules for (status, reason) combinations.
    """

    @staticmethod
    def from_success_reason(reason: SliceStatusReason) -> SliceStatus:
        """
        Resolve SliceStatus for a successful (HTTP 200, parsed) slice fetch.

        Raises:
            ValueError if the reason is not valid on the success path.
        """
        if reason in (
            SliceStatusReason.NONE,
            SliceStatusReason.HISTORICAL_SPARSITY, # This is still considered complete because it's an datasource REST API data quality issue, nothing we can do to fix it
        ):
            return SliceStatus.COMPLETE

        if reason in (
            SliceStatusReason.STILL_FORMING,
            SliceStatusReason.AWAITING_UPSTREAM,
        ):
            return SliceStatus.PARTIAL

        raise ValueError(
            f"Invalid success-path SliceStatusReason: {reason}"
        )

    @staticmethod
    def from_failure_reason(reason: SliceStatusReason) -> SliceStatus:
        """
        Resolve SliceStatus for a failed slice fetch.

        Raises:
            ValueError if the reason is not valid on the failure path.
        """
        if reason in (
            SliceStatusReason.HTTP_ERROR,
            SliceStatusReason.EXCEPTION,
        ):
            return SliceStatus.FAILED

        raise ValueError(
            f"Invalid failure-path SliceStatusReason: {reason}"
        )
