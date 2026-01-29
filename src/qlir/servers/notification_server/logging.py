import logging

from qlir.servers.notification_server.adapters.base import NotificationAdapter


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger("notification-server")



def log_outbox_adapters(outbox_adapters: dict[str, list[NotificationAdapter]]):
    
    logdf(data=NamedDF(list_to_df(outbox_adapters), "outbox routes")) #type: ignore 