import asyncio
import logging
import uuid
from asyncio import TaskGroup
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Union, Iterable

from lib.queue_controller.queueController import QueueController
from lib.queue_controller.queueData import QueueData

queue_action_typehint = Callable[[QueueData], Union[Exception | None]]
queue_action_errors_typehint = Callable[[Exception], bool]

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def simple_error_handler(e: Exception) -> bool:
    logger.error("An error occurred during queue execution", exc_info=True)
    return True

def link_pipeline(nodes: Iterable[QueueController]) -> None:
    node_list = list(nodes)

    if not node_list:
        return

    for i in range(len(node_list)-1):
        current_node = node_list[i]
        next_node = node_list[i+1]
        current_node.set_next(next_node)


def start_pipeline(tg: TaskGroup, nodes: list[QueueController]) -> list[asyncio.Task]:
    return [tg.create_task(node.queue_action()) for node in nodes]

def gather_results(futures: list[Future]):
    return [f.result() for f in futures]

async def stop_pipeline(nodes: list[QueueController]) -> None:
    for node in nodes:
        await node.close()

def default_queue_action(queue_data: QueueData) -> None:
    pass

def new_controller(identity: str = None, executor: ThreadPoolExecutor = None, action: Callable[[QueueData], asyncio.Future] = None, **kwargs) -> QueueController:
    if action is None:
        action = default_queue_action

    if identity is None:
        identity = f"{uuid.uuid4().hex}-{action.__name__}"

    def _controller() -> QueueController:
        return QueueController(identity=identity, action=action, executor=executor, **kwargs)

    return _controller()

