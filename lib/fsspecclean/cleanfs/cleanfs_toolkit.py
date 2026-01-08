
import io
from typing import List, Annotated, Any

import pandas as pd
from langchain_core.tools import BaseToolkit, tool
from langgraph.prebuilt import InjectedState
from pydantic import ConfigDict

from lib.fsspecclean.cleanfs.cleanfs import CleanFs

def _validate_request_id(tool_name: str, state: Annotated[dict, InjectedState]):
    request_id = state.get("request_id")
    if not request_id:
        raise KeyError(
            f"Execution halted: Tool '{tool_name}' requires 'request_id' in state. "
        )
    return request_id

def _validate_csv_data(tool_name: str, state: Annotated[dict, InjectedState]):
    request_id = _validate_request_id(tool_name, state)
    csv_data = state.get("csv_data")
    if not csv_data:
        raise AttributeError(
            f"Execution halted: Tool '{tool_name}' requires 'csv_data' in state. "
            f"Request ID: {request_id}"
        )
    return request_id, csv_data

def _retrieve(get_file, tool_name, state):
    try:
        request_id = _validate_request_id(tool_name, state)
        df = get_file(request_id)

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8')
        raw_bytes = buffer.getvalue()

        artifact = {
            "data": str(raw_bytes),
            "type": "binary"
        }

        return df.to_csv(index=False), artifact
    except AttributeError:
        pass
    except Exception:
        raise

class CleanFSToolkit(BaseToolkit):

    fs: CleanFs

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> List:

        # We wrap the methods in functions to use the @tool decorator
        @tool(response_format="content_and_artifact")
        def get_clean_file(state: Annotated[dict, InjectedState] ) -> tuple[str, tuple[Any, dict[str, str]] | None]:
            """Retrieve and read the cleaned CSV file for a specific request ID."""
            fn = self.fs.get_clean_file
            try:
                opres = _retrieve(fn, fn.__name__, state)
                return str(opres), opres
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def get_raw_file(state: Annotated[dict, InjectedState]) -> tuple[str, tuple[Any, dict[str, str]] | None]:
            """Retrieve and read the raw CSV file for a specific request ID."""
            fn = self.fs.get_raw_file
            try:
                opres = _retrieve(fn, fn.__name__, state)
                return str(opres), opres
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def save_clean_file(state: Annotated[dict, InjectedState]):
            """Save cleaned data to the filesystem. Input must be a CSV-formatted string."""
            fn = self.fs.save_clean_file
            try:
                request_id, csv_data = _validate_csv_data(fn.__name__, state)
                df = pd.read_csv(io.StringIO(csv_data))
                fn(request_id, df, use_pipe=True)
                resp = f"Successfully saved clean file for {request_id}"
                return resp, True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def save_raw_file(state: Annotated[dict, InjectedState] ):
            """Saves files specifically requiring CSV formatting data."""
            fn = self.fs.save_raw_file
            try:
                request_id, csv_data = _validate_csv_data(fn.__name__, state)
                df = pd.read_csv(io.StringIO(csv_data))
                fn(request_id, df, use_pipe=True)
                resp = f"Successfully saved raw file for {request_id}"
                return resp, True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def list_raw_files(state: Annotated[dict, InjectedState] ) -> tuple[str, list[Any]] | None:
            """List all raw files available for a given request ID."""
            fn = self.fs.list_raw_files
            try:
                opres = list(fn(_validate_request_id(fn.__name__, state)))
                return str(opres), opres
            except AttributeError:
                pass
            except KeyError:
                raise

        @tool(response_format="content_and_artifact")
        def list_clean_files(state: Annotated[dict, InjectedState] ) -> tuple[str, list[Any]] | None:
            """List all clean files available for a given request ID."""
            fn = self.fs.list_clean_files
            try:
                opres = list(fn(_validate_request_id(fn.__name__, state)))
                return str(opres), opres
            except AttributeError:
                pass
            except KeyError:
                raise

        return [
            get_clean_file, get_raw_file, save_clean_file,
            save_raw_file, list_raw_files, list_clean_files
        ]