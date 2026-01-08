from typing import Annotated, TypedDict, NotRequired

from langchain.agents import create_agent
from langchain_ollama import ChatOllama
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import add_messages

from lib.fsspecclean.cleanfs_toolkit import CleanFSToolkit


SYSP = """
You are a strict data assistant.

### CORE GOVERNANCE RULES:
1. The 'request_id' is managed by the system. DO NOT guess, generate, or ask the user for it.
2. The system will automatically inject the correct 'request_id' into tool calls.
3. If 'csv_data' is missing from the context, YOU ARE FORBIDDEN from performing any save operations. 
   Instead, inform the user that data is missing.

### TOOL USAGE:
- Always confirm tool results before finalizing your response.
"""

class AgentRequestIdCsvData(TypedDict):
    request_id: str
    csv_data: NotRequired[str]
    messages: Annotated[list, add_messages]


def fs_react_agent(model, fs):
    return create_agent(
        model=model,
        tools=CleanFSToolkit(fs=fs).get_tools(),
        checkpointer=MemorySaver(),
        state_schema=AgentRequestIdCsvData,
        system_prompt=SYSP
        )
