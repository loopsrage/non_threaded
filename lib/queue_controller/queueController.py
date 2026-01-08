import logging
import queue
import threading
import traceback
from concurrent import futures
from typing import Optional, Callable, Union

from lib.queue_controller.queueData import QueueData
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def debug_action(item: QueueData) -> None:
    print(item)

def handle_error(e: Exception) -> bool:
    traceback.print_exception(e)
    logger.error("An error occurred during queue execution", exc_info=True)
    return False

class QueueController:
    _identity: str
    _queue: queue.Queue = None
    _broadcast: dict[str, 'QueueController']

    _action: Callable[[QueueData], Union[Exception, None]]
    _next_queue_controller: Optional['QueueController'] = None
    _error_handler: Callable[[Exception], bool]
    _lock: threading.Lock
    _executor: futures.ThreadPoolExecutor

    def __init__(self, identity: str,
                 action: Callable[[QueueData], Union[Exception, None]],
                 executor: futures.ThreadPoolExecutor = None,
                 max_queue_size: int = None,
                 error_handler: Callable[[Exception], bool] = None) -> None:

        self._error_handler = error_handler
        if self._error_handler is None:
            self._error_handler = handle_error

        if max_queue_size is None:
            max_queue_size = 1024

        self._max_queue_size = max_queue_size
        self._identity = identity
        self._lock = threading.Lock()
        self._action = action
        self._broadcast = {}

        self._executor = executor
        if self._executor is None:
            self._executor = futures.ThreadPoolExecutor()

    @property
    def identity(self):
        with self._lock:
            if self._identity is None:
                return ""
            return self._identity

    @property
    def queue(self) -> queue.Queue:
        if self._queue is None:
            # queue.Queue must be created within the event loop
            self._queue = queue.Queue(maxsize=self._max_queue_size)
        return self._queue

    @property
    def next_queue_controller(self) -> Union['QueueController', None]:
       return self._next_queue_controller

    def set_next(self, next_queue_controller: 'QueueController') -> None:
        self._next_queue_controller = next_queue_controller

    def set_broadcast(self, broadcast_to: dict[str, 'QueueController']) -> None:
        with self._lock:
            self._broadcast = broadcast_to

    def enqueue(self, queue_data: QueueData) -> None:
        self.queue.put(queue_data)

    def close(self) -> None:
        self.queue.put(None)
        self.queue.join()
        self._executor.shutdown(wait=True)

    def broadcast(self, item) -> None:
        with self._lock:
            targets = self._broadcast.items()

        for identity, target in targets:
            target.enqueue(item.copy_derivative(identity))

    def queue_action(self) -> None:
        while True:
            item: QueueData = self.queue.get()
            if item is None:
                self.queue.task_done()
                return

            item.append_trace(self.identity)

            try:
                future = self._executor.submit(self._action, item)
                self.broadcast(item)

                result = future.result()
                if isinstance(result, Exception):
                    raise result

                next_node = self.next_queue_controller
                if next_node:
                    next_node.enqueue(item)
            except Exception as e:
                e.add_note(f"{item.trace()}")
                e.add_note(f"{item.all_attributes()}")
                if not self._error_handler(e):
                    raise e
            finally:
                self.queue.task_done()

