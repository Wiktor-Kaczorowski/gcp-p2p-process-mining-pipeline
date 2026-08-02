from pathlib import Path
import sys

import pandas as pd
import pm4py
import pyarrow


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]

    print("Environment check")
    print("-----------------")
    print(f"Project directory: {project_root}")
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Pandas version: {pd.__version__}")
    print(f"PM4Py version: {pm4py.__version__}")
    print(f"PyArrow version: {pyarrow.__version__}")
    print("\nEnvironment configured successfully.")


if __name__ == "__main__":
    main()