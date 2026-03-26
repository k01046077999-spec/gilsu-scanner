from threading import RLock
from typing import Any


class InMemoryCache:
    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._lock = RLock()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._data.get(key)
            if value is None:
                return None
            return dict(value)

    def set(self, key: str, value: dict[str, Any]) -> None:
        with self._lock:
            self._data[key] = dict(value)
