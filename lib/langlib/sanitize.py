from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

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