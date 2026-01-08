from abc import ABC, abstractmethod
from typing import Any


class NotificationAdapter(ABC):
    @abstractmethod
    def send(self, data: Any) -> None:
        """
        Send the alert payload.
        Raise on failure.
        """
        ...
