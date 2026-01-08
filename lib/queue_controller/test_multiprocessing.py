import sys
import time
import unittest
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import pytest

from lib.index import Index
from helpers import QueueData, link_pipeline, stop_pipeline, start_pipeline, gather_results, \
    new_controller
from lib.onceler import Onceler
from lib.stats_collector.stats_collector import aggregate_action

once = Onceler()
stats = Index()

def do_expensive_operation_once():
    def once_callback():
        time.sleep(10)
        print("EXPENSIVE OPERATION")

    once_callback()


class Test(unittest.IsolatedAsyncioTestCase):

    def test_queue_action_link(self):
        with ThreadPoolExecutor() as executor:
            new_with_executor = partial(new_controller)
            pl = []
            for i in range(18):
                pl.append(new_with_executor())

            pl.append(new_with_executor(identity="agg", action=aggregate_action))
            link_pipeline(nodes=pl)

            try:
                worker_tasks = start_pipeline(executor=executor, nodes=pl)
                for j in range(600):
                    pl[0].enqueue(QueueData())
                    pl[1].enqueue(QueueData())

            except ExceptionGroup as eg:
                pytest.fail(f"Pipeline node failed: {eg}")
            finally:
                stop_pipeline(nodes=pl)
                gather_results(worker_tasks)


    def test_queue_action_broadcast(self):
        with ThreadPoolExecutor() as executor:
            new_with_executor = partial(new_controller)
            m1 = new_with_executor(identity="m1")
            m4 = new_with_executor(identity="m4")
            m5 = new_with_executor(identity="m5")
            m6 = new_with_executor(identity="m6")
            agg = new_with_executor(identity="agg")
            pl = [m1, m4, m5, m6, agg]

            worker_tasks = start_pipeline(executor=executor, nodes=pl)
            m1.set_broadcast({
                "Derivative_0": m4,
                "Derivative_1": m5,
                "Derivative_2": m6,
            })
            m6.set_next(agg)
            try:
                for j in range(3):
                    m1.enqueue(QueueData())
            except ExceptionGroup as eg:
                # TaskGroup surfaces any node failures here
                pytest.fail(f"Pipeline node failed: {eg}")
            finally:
                stop_pipeline(nodes=pl)
                gather_results(worker_tasks)


    def test_queue_action_complex(self):
        with ThreadPoolExecutor() as executor:
            new_with_executor = partial(new_controller)
            m0 = new_with_executor(identity="m0")
            m1 = new_with_executor(identity="m1")
            m2 = new_with_executor(identity="m2")
            m3 = new_with_executor(identity="m3")
            m4 = new_with_executor(identity="m4")
            m5 = new_with_executor(identity="m5")
            m6 = new_with_executor(identity="m6")
            m9 = new_with_executor(identity="m9")
            m7 = new_with_executor(identity="m7")
            m8 = new_with_executor(identity="m8")
            agg = new_with_executor(identity="agg", action=aggregate_action)

            pl = [m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, agg]

            m0.set_broadcast({
                "Derivative_1": m1,
                "Derivative_2": m2,
                "Derivative_3": m3,
            })

            for m in [m1, m2, m3]:
                m.set_broadcast({
                    "Derivative_4": m4,
                    "Derivative_5": m5,
                    "Derivative_6": m6,
                })

            link_pipeline(nodes=[m7, m8, m9])
            for i in [m4, m5, m6]:
                i.set_next(m7)

            m9.set_next(agg)
            worker_tasks = start_pipeline(nodes=pl, executor=executor)
            print(f"Is GIL enabled: {sys._is_gil_enabled()}")
            try:
                for j in range(10):
                    m0.enqueue(QueueData())
                for k in range(600):
                    m1.enqueue(QueueData())
                    m2.enqueue(QueueData())
                    m3.enqueue(QueueData())

                for k in range(500):
                    m7.enqueue(QueueData())

            except ExceptionGroup as eg:
                # TaskGroup surfaces any node failures here
                pytest.fail(f"Pipeline node failed: {eg}")
            finally:
                stop_pipeline(nodes=pl)
                gather_results(worker_tasks)
