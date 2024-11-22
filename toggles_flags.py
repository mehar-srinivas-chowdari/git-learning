import os
import threading

class FlagManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(FlagManager, cls).__new__(cls)
                    cls._instance._flags = {}
                    cls._instance._lock = threading.Lock()
        return cls._instance

    def initialize_flags(self, flags: dict):
        """
        Initialize flags with default values from environment variables or given dictionary.
        """
        with self._lock:
            for flag, default_value in flags.items():
                self._flags[flag] = os.getenv(flag.upper(), str(default_value)).lower() == "true"

    def get_flag(self, name: str) -> bool:
        """
        Get the value of a flag.
        """
        with self._lock:
            return self._flags.get(name, False)

    def set_flag(self, name: str, value: bool):
        """
        Set the value of a flag.
        """
        with self._lock:
            self._flags[name] = value

    def toggle_flag(self, name: str) -> bool:
        """
        Toggle the value of a flag.
        """
        with self._lock:
            if name in self._flags:
                self._flags[name] = not self._flags[name]
            else:
                self._flags[name] = True  # Default to True if not initialized
            return self._flags[name]

    def get_all_flags(self) -> dict:
        """
        Get all flags and their values.
        """
        with self._lock:
            return self._flags.copy()


# Global instance for convenience
flag_manager = FlagManager()