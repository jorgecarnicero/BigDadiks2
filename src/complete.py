#!/usr/bin/env python3
"""
Wrapper to reuse the existing complete.py from src/ in the testing/opencode folder.
If you want to modify ingestion params, edit the original src/complete.py.
"""

import runpy


if __name__ == "__main__":
    runpy.run_module("src.complete", run_name="__main__")
