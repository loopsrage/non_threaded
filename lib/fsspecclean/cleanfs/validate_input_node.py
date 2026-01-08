from lib.fsspecclean.cleanfs.cleanfs_tool_agent import AgentRequestIdCsvData

def validate_node(state: AgentRequestIdCsvData):
    request_id = state.get('request_id')
    if not request_id:
        return {"governance_fail": True}

    user_input = state["messages"][-1].content.lower()
    if "save" in user_input and not state.get("csv_data"):
        return {"governance_fail": True}

    return {"governance_fail": False}
