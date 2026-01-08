from graphlib import TopologicalSorter
import pandas as pd

def topsort_json_paths(df, delim = None):
    if delim is None:
        delim = "."

    df = pd.json_normalize(df, sep=delim)
    graph = {}
    for path in df.columns:
        if delim in path:
            # Recursively ensure all parent levels are in the graph
            # e.g. "a.b.c" -> "a.b" depends on "a", "a.b.c" depends on "a.b"
            parts = path.split(delim)
            for i in range(1, len(parts)):
                child = delim.join(parts[:i+1])
                parent = delim.join(parts[:i])
                if child not in graph:
                    graph[child] = set()
                graph[child].add(parent)
                if parent not in graph:
                    graph[parent] = set()
        else:
            if path not in graph:
                graph[path] = set()

    ts = TopologicalSorter(graph)
    # static_order() returns an iterator of paths in dependency order
    try:
        sorted_columns = list(ts.static_order())
    except Exception as e:
        raise

    final_order = [col for col in sorted_columns if col in df.columns]
    return df[final_order], final_order

def process_json(data, delim=None) -> tuple[pd.DataFrame, list[str]]:
    try:
        return topsort_json_paths(data, delim)
    except Exception:
        raise
