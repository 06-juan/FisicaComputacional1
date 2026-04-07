from __future__ import annotations
from pathlib import Path

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

def data_dir() -> Path:
    return project_root() / "data"

def bronze_dir() -> Path:
    return data_dir() / "bronze"

def silver_dir() -> Path:
    return data_dir() / "silver"

def gold_dir() -> Path:
    return data_dir() / "gold"

def duckdb_path() -> Path:
    return data_dir() / "Clima.duckdb"
