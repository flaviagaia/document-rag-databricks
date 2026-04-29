from __future__ import annotations

import json

from src.rag_pipeline import run_pipeline


if __name__ == "__main__":
    print(json.dumps(run_pipeline(), indent=2))
