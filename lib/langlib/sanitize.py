from graphlib import TopologicalSorter
from typing import TypedDict

import pandas as pd
from langgraph.graph import StateGraph, START, END
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

# CONTEXT
# AUTH
# SELF AWARENESS
# GAURDRAILS
# MIDDLEWARE
# REMOTE RETRIEVAL
#
# EMBED AS YOU GO
# CHUNK AS YOU GO

# VECTOR SEARCH -> INJECT INTO MODEL CONTEXT

# EXTRACT STRUCTURED DATA BASED ON CATEGORY: METADATA
# CLASSIFY NEW CATEGORIES - CATEGORY DISCOVERY
# SAVE AS ARTIFACTS, INJECT INTO MODEL CONTEXT -

def topsort_json_paths(df, delim = None):
    if delim is None:
        delim = "."

    df = pd.json_normalize(df, sep=delim)
    graph = {}
    for path in df.columns:
        # If the path has delim, the parent is the substring before the last delim
        if delim in path:
            parent = path.rsplit(delim, 1)[0]
            graph[path] = {parent}
        else:
            # Root level paths have no dependencies
            graph[path] = set()

    ts = TopologicalSorter(graph)
    # static_order() returns an iterator of paths in dependency order
    try:
        sorted_columns = list(ts.static_order())
    except Exception as e:
        raise

    final_order = [col for col in sorted_columns if col in df.columns]
    return df[final_order]

def process_json(data, delim=None):
    try:
        topsort_json_paths(data, delim)
    except Exception:
        raise


# 1. Define the State

class GraphState(TypedDict):
    raw_text: str
    redacted_text: str

# 2. Define the Presidio Node
def pii_redaction_node(state: GraphState):
    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()

    # Analyze for PII
    results = analyzer.analyze(text=state["raw_text"], language='en')

    # Anonymize/Redact
    anonymized_result = anonymizer.anonymize(
        text=state["raw_text"],
        analyzer_results=results
    )

    # Return updated state
    return {"redacted_text": anonymized_result.text}

if __name__ == "__main__":
    workflow = StateGraph(GraphState)
    workflow.add_node("redactor", pii_redaction_node)

    workflow.add_edge(START, "redactor")
    workflow.add_edge("redactor", END)

    app = workflow.compile()

    # 4. Run it
    output = app.invoke({"raw_text": "My name is John Doe and my number is 555-1234"})
    print(output["redacted_text"])