import json
import unittest
import uuid

from langchain_core.messages import ToolMessage
from langchain_ollama import ChatOllama

from lib.fsspecclean import FSpecFS
from lib.fsspecclean.cleanfs_graph import build_governed_graph
from lib.fsspecclean.cleanfs_tool_agent import fs_react_agent


class Test(unittest.TestCase):

    def test_build_governed_node(self):
        my_fs = FSpecFS(filesystem="memory")
        artifacts = []
        all_all_messages = []
        request_ids = []
        llm = ChatOllama(model="functiongemma")
        worker_agent = fs_react_agent(llm, my_fs)
        graph = build_governed_graph(worker_agent)

        for i in range(0, 10):
            request_id = uuid.uuid4().hex
            request_ids.append(request_id)
            config = {"configurable": {"thread_id": request_id}}
            commands = [
                {"messages": [("user", "save files")], "request_id": request_id},  # Fails (No csv_data)
                {"messages": [("user", "save raw files")], "request_id": request_id, "csv_data": "0,1,1,3,1"},
                {"messages": [("user", "save clean files")], "request_id": request_id, "csv_data": "0,1,1,3,1"},
                {"messages": [("user", "list raw files")], "request_id": request_id},
                {"messages": [("user", "list clean files")], "request_id": request_id},
                {"messages": [("user", "get raw file")], "request_id": request_id},
                {"messages": [("user", "get clean file")], "request_id": request_id},
            ]
            for cmd in commands:
                try:
                    _result = graph.invoke(cmd, config=config)
                except ValueError as e:
                    pass

            final_state = graph.get_state(config)
            all_messages = final_state.values["messages"]
            all_all_messages.append(all_messages)

            # Retrieve the Artifact from the final ToolMessage in history
            artifact = next(
                (msg.artifact for msg in reversed(all_messages)
                 if isinstance(msg, ToolMessage) and getattr(msg, "artifact", None)),
                None
            )

            if artifact:
                artifacts.append(artifact)

        print(
            f"RequestIds: {len(request_ids)}, Artifacts: {len(artifacts)}, Messages: {len(all_all_messages)}"
        )

        report = {}
        for rid in request_ids:
            _raw = {"raw": []}
            _clean = {"clean": []}

            for i in my_fs.list_raw_files(rid):
                _raw["raw"].append(i)

            for i in my_fs.list_clean_files(rid):
                _clean["clean"].append(i)

            report[rid] = {**_raw, **_clean}

        print(artifacts, all_all_messages)
        print(json.dumps(report, indent=4))
