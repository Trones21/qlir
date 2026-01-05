import logging


def ensure_logging() -> None:
    """
    Ensure there is *some* logging configuration so logdf
    doesn't silently drop messages in ad-hoc scripts / notebooks.

    Priority:
      1. If the qlir logger has handlers, trust that setup.
      2. Else if root has handlers, trust that.
      3. Else configure a simple root handler at INFO.
    """
    root = logging.getLogger()
    qlir_logger = logging.getLogger("qlir")

    # If either qlir or root already has handlers, don't touch config.
    if qlir_logger.hasHandlers() or root.hasHandlers():
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print("[init] Logging configured (default INFO)")