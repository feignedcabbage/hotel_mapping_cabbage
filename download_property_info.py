#!/usr/bin/env python3
"""Thin wrapper — logic lives in hotelmap/download.py (importable by the pipeline).

Usage:
    .venv/bin/python download_property_info.py [--country IN]
"""

import sys

from hotelmap.download import main

if __name__ == "__main__":
    sys.exit(main())
