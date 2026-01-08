from langgraph.checkpoint.memory import MemorySaver
from langgraph.constants import START, END
from langgraph.graph import StateGraph

from lib.fsspecclean.memfs_tool_node import AgentRequestIdCsvData
from lib.fsspecclean.validate_input_node import validate_node


def build_governed_graph(worker_agent):
    # Initialize the worker agent

    # Create the Parent workflow
    workflow = StateGraph(AgentRequestIdCsvData)

    workflow.add_node("governance_gate", validate_node)
    workflow.add_node("worker", worker_agent)

    workflow.add_edge(START, "governance_gate")
    workflow.add_conditional_edges(
        "governance_gate",
        route_after_gate,
        {
            "worker": "worker",
            END: END
        }
    )

    # 4. Worker always goes to END once finished
    workflow.add_edge("worker", END)

    return workflow.compile(checkpointer=MemorySaver())

def route_after_gate(state):
    if state.get("governance_fail"): return END
    return "worker"