import threading
from typing import Any, Callable
from lib.index import Index


class Onceler:
    def __init__(self):
        self.index_manager = Index()
        # We need another index to store the synchronization primitives (Locks)
        # Ensure our indexes exist
        self.index_manager.new("results")
        self.index_manager.new("locks")

    def store_once(self, index_name: str, key: Any, do: Callable[[], Any]) -> Any:
        """
        Ensures the 'do' function runs only once for a given index_name/key pair, 
        and stores the resulting value in the index manager.
        """
        full_key = f"{index_name}:{key}"

        actual_lock, was_loaded = self.index_manager.load_or_store_in_index(
            index_name="locks",
            key=f"{index_name}:{key}",
            value=threading.Lock()
        )

        results_data, _ = self.index_manager.get_index_and_lock("results")
        if full_key in results_data:
            return self._handle_result(results_data[full_key])

        with actual_lock:
            results_data, loaded = self.index_manager.get_index_and_lock("results")
            if full_key in results_data:
                # The value is already computed and stored.
                value = results_data[full_key]
                if isinstance(value, Exception):

                    # re raise the exception
                    raise value
                return value

            try:
                result = do()
                self.index_manager.store_in_index(
                    index_name="results",
                    key=full_key,
                    value=result if result is not None else "COMPLETED"
                )
                return result
            except Exception as e:
                self.index_manager.store_in_index(
                    index_name="results",
                    key=full_key,
                    value=e
                )
                raise

    def _handle_result(self, value: Any) -> Any:
        if isinstance(value, Exception):
            raise value
        return value