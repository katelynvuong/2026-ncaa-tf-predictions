#!/usr/bin/env python3
"""Materialize 2026 championship assets."""
import sys
from pathlib import Path
from pipeline import defs
from dagster import materialize

# Materialize the championship assets
print("Materializing 2026 championship assets...")
result = materialize(
    [*defs.assets],
    raise_on_error=False
)

if result.success:
    print("✓ Championship assets materialized successfully")
    sys.exit(0)
else:
    print("✗ Error materializing assets")
    sys.exit(1)
