# src/duckdb_functions.py
import duckdb
import numpy as np
import pyarrow as pa  # Để hỗ trợ Arrow cho vectorized UDFs
from typing import Callable, List, Any
from duckdb import DuckDBPyConnection
from duckdb.typing import DuckDBPyType, FLOAT, VARCHAR, DOUBLE
from duckdb.functional import PythonUDFType  # Để set type cho UDF


class DuckDBFunctions:
    """
    Utility class for managing custom Python UDFs (User-Defined Functions) in DuckDB.

    Attributes
    ----------
    con : DuckDBPyConnection
        Active DuckDB connection.
    registered_functions : dict
        Registry of all user-defined functions added during the session.
    """

    def __init__(self, con: DuckDBPyConnection):
        if not isinstance(con, DuckDBPyConnection):
            raise TypeError("A valid DuckDB connection is required.")
        self.con = con
        self.registered_functions = {}

    # -----------------------------------------------------
    # UDF Registration
    # -----------------------------------------------------
    def register_udf(
        self,
        name: str,
        function: Callable,
        parameters: List[DuckDBPyType],
        return_type: DuckDBPyType,
        vectorized: bool = False,
        side_effects: bool = False,
    ):
        """
        Register a Python function as a DuckDB UDF (User-Defined Function).

        Parameters
        ----------
        name : str
            The name to register the function under in DuckDB.
        function : Callable
            The Python function to expose to DuckDB.
        parameters : list[duckdb.DuckDBPyType]
            A list of input parameter types.
        return_type : duckdb.DuckDBPyType
            The DuckDB return type of the UDF.
        vectorized : bool, optional
            If True, registers as vectorized UDF using ARROW type.
        side_effects : bool, optional
            If True, allows the function to have side effects.

        Raises
        ------
        ValueError
            If the function name already exists or parameters are invalid.
        RuntimeError
            If the function registration fails.
        """

        if name in self.registered_functions:
            raise ValueError(f"Function '{name}' is already registered.")

        try:
            # Set UDF type: ARROW for vectorized, NATIVE for scalar
            udf_type = PythonUDFType.ARROW if vectorized else PythonUDFType.NATIVE

            # Register the UDF in DuckDB
            self.con.create_function(
                name=name,
                function=function,  # Đúng kwarg: function
                parameters=parameters,
                return_type=return_type,
                type=udf_type,
                side_effects=side_effects,
            )

            # Store metadata for tracking
            self.registered_functions[name] = {
                "params": parameters,
                "return_type": return_type,
                "vectorized": vectorized,
            }

        except Exception as e:
            raise RuntimeError(f"Failed to register UDF '{name}': {e}")

    # -----------------------------------------------------
    # UDF Removal
    # -----------------------------------------------------
    def remove_udf(self, name: str):
        """
        Remove a registered UDF by name.

        Parameters
        ----------
        name : str
            The name of the UDF to remove.

        Raises
        ------
        KeyError
            If the UDF does not exist.
        RuntimeError
            If removal fails internally.
        """
        if name not in self.registered_functions:
            raise KeyError(f"UDF '{name}' is not registered.")

        try:
            self.con.remove_function(name)
            del self.registered_functions[name]
        except Exception as e:
            raise RuntimeError(f"Failed to remove UDF '{name}': {e}")

    # -----------------------------------------------------
    # UDF Listing
    # -----------------------------------------------------
    def list_udfs(self) -> dict:
        """
        Return a dictionary of all registered UDFs.

        Returns
        -------
        dict
            Mapping of UDF names to their metadata.
        """
        return self.registered_functions

    # -------------------------------------------------------------------------
    # Context Manager Support
    # -------------------------------------------------------------------------
    def __enter__(self):
        """Allow usage via 'with DuckDBFunctions(con) as udf:' syntax."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Automatically clean up registered UDFs when exiting context."""
        try:
            for name in list(self.registered_functions.keys()):
                self.con.remove_function(name)
            self.registered_functions.clear()
        except Exception as e:
            print(f"[Warning] Failed to clean up UDFs: {e}")


# con = duckdb.connect(":memory:")


# # --- Test context manager ---
# def test_udf_context_manager():
#     with DuckDBFunctions(con) as udf:
#         # Register a simple UDF: square a number (vectorized with Arrow)
#         def square_arrow(arr: pa.Array) -> pa.Array:
#             # Use pyarrow compute for native performance
#             arr_float = arr.cast(pa.float64())
#             return pa.compute.multiply(arr_float, arr_float)

#         udf.register_udf(
#             name="square",
#             function=square_arrow,
#             parameters=[DOUBLE],
#             return_type=DOUBLE,
#             vectorized=True,
#         )

#         # Use the UDF in a query
#         result = con.execute(
#             "SELECT square(x) AS sq FROM (VALUES (2), (3), (4)) AS t(x)"
#         ).fetchall()
#         print("Query result using UDF:", result)
#         # Expected: [(4.0,), (9.0,), (16.0,)]

#         # List registered UDFs inside context
#         print("Registered UDFs:", udf.list_udfs())

#     # After context, the UDF should be removed automatically
#     try:
#         con.execute("SELECT square(2)").fetchall()
#     except Exception as e:
#         print("Expected error after context exit:", e)


# # --- Run the test ---
# if __name__ == "__main__":
#     test_udf_context_manager()
