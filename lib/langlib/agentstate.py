from typing import Literal

from langgraph.constants import START, END
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field


class AgentState(BaseModel):
    # Primary data
    user_input: str
    sanitized_query: str

    # Governance Metadata (Assigned during initialization)
    user_role: Literal["admin", "employee", "guest"] = "guest"
    data_sensitivity: Literal["public", "internal", "confidential"] = "public"

    # Audit Trail
    risk_score: float = Field(default=0.0, description="Assessed risk level (0-1)")
    governance_approved: bool = False

# 2. Initialization Node (The Policy Binder)
def initialize_governance_state(state: AgentState):
    """
    Acts as the entry point after sanitization.
    It 'binds' the user's role and calculates initial risk.
    """
    # In a real app, you'd fetch 'user_role' from your auth system
    # This step is critical for Step 2 (Deterministic Routing)

    updates = {
        "user_role": "employee",  # Example: Bound from system context
        "data_sensitivity": "internal",
        "risk_score": 0.2
    }
    return updates

# 3. Build the Graph with Checkpointing
builder = StateGraph(AgentState)
builder.add_node("initialize", initialize_governance_state)

# Set the flow
builder.add_edge(START, "initialize")
builder.add_edge("initialize", END)

# Compile with a checkpointer for an immediate audit trail
# This creates a persistent record of the state at this exact step
from langgraph.checkpoint.memory import MemorySaver
memory = MemorySaver()
graph = builder.compile(checkpointer=memory)

# Execute
config = {"configurable": {"thread_id": "audit_trail_001"}}
initial_input = {"user_input": "Sanitized prompt here", "sanitized_query": "sanitized_query"}
graph.invoke(initial_input, config=config)