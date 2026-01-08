
import io
from typing import List, Annotated, Any

import pandas as pd
from langchain_core.tools import BaseToolkit, tool
from langgraph.prebuilt import InjectedState
from pydantic import ConfigDict

from lib.fsspecclean import FSpecFS

class FSspecToolKit(BaseToolkit):

    fs: FSpecFS

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> List:

        # We wrap the methods in functions to use the @tool decorator
        @tool(response_format="content_and_artifact")
        def get_clean_file(state: Annotated[dict, InjectedState] ) -> tuple[Any, dict[str, bytes]]:
            """Retrieve and read the cleaned CSV file for a specific request ID."""
            df = self.fs.get_clean_file(state.get("request_id"))

            buffer = io.BytesIO()
            # index=False is usually preferred for clean data transfer
            df.to_csv(buffer, index=False, encoding='utf-8')
            raw_bytes = buffer.getvalue()

            artifact = {
                "raw_data": raw_bytes,
                "type": "binary"
            }

            return df.to_csv(index=False), artifact

        @tool(response_format="content_and_artifact")
        def get_raw_file(state: Annotated[dict, InjectedState]) -> tuple[Any, dict[Any, bytes]]:
            """Retrieve and read the raw CSV file for a specific request ID."""
            request_id = state.get("request_id")
            df = self.fs.get_raw_file(request_id)

            buffer = io.BytesIO()
            # index=False is usually preferred for clean data transfer
            df.to_csv(buffer, index=False, encoding='utf-8')
            raw_bytes = buffer.getvalue()

            artifact = {
                "raw_data": raw_bytes,  # This is the 'data' you retrieve later
                "type": "binary"
            }

            return df.to_csv(index=False), artifact

        @tool(response_format="content")
        def save_clean_file(state: Annotated[dict, InjectedState]):
            """Save cleaned data to the filesystem. Input must be a CSV-formatted string."""
            request_id = state.get("request_id")
            csv_data = state.get("csv_data")

            if not csv_data:
                raise KeyError(
                    f"Execution halted: Tool 'save_csv_files' requires 'csv_data' in state. "
                    f"Request ID: {request_id}"
                )

            df = pd.read_csv(io.StringIO(csv_data))
            self.fs.save_clean_file(request_id, df, use_pipe=True)
            return f"Successfully saved clean file for {request_id}"

        @tool(response_format="content")
        def save_raw_file(state: Annotated[dict, InjectedState] ):
            """Saves files specifically requiring CSV formatting data."""
            request_id = state.get("request_id")
            csv_data = state.get("csv_data")

            if not csv_data:
                raise KeyError(
                    f"Execution halted: Tool 'save_csv_files' requires 'csv_data' in state. "
                    f"Request ID: {request_id}"
                )

            df = pd.read_csv(io.StringIO(csv_data))
            self.fs.save_raw_file(df, request_id, use_pipe=True)
            return f"Successfully saved raw file for {request_id}"

        @tool(response_format="content")
        def list_raw_files(state: Annotated[dict, InjectedState] ) -> List[str]:
            """List all raw files available for a given request ID."""
            return list(self.fs.list_raw_files(state.get("request_id")))

        @tool(response_format="content")
        def list_clean_files(state: Annotated[dict, InjectedState] ) -> List[str]:
            """List all clean files available for a given request ID."""
            return list(self.fs.list_clean_files(state.get("request_id")))

        @tool(response_format="content")
        def list_images(state: Annotated[dict, InjectedState]) -> List[str]:
            """List all PNG image paths in the images subdirectory for a request ID."""
            return list(self.fs.list_images(state.get("request_id")))

        # Note: save_png_file is typically handled by a specialized node
        # because LLMs cannot pass an active 'figure' object directly.

        return [
            get_clean_file, get_raw_file, save_clean_file,
            save_raw_file, list_raw_files, list_clean_files, list_images
        ]