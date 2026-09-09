"""
Lightweight data quality checks run against a pandas DataFrame *before* it is
loaded into the warehouse. Results are returned as a list of dicts so the
caller can decide whether to fail the load, quarantine rows, or just log a
warning — and so every check gets written to meta.data_quality_results for
observability.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass
class DQResult:
    check_name: str
    table_name: str
    passed: bool
    failed_rows: int = 0
    details: str = ""


def check_not_null(df: pd.DataFrame, table_name: str, columns: list[str]) -> list[DQResult]:
    results = []
    for col in columns:
        if col not in df.columns:
            results.append(DQResult(f"not_null:{col}", table_name, False, len(df), f"column {col} missing"))
            continue
        n_null = int(df[col].isna().sum())
        results.append(DQResult(
            check_name=f"not_null:{col}",
            table_name=table_name,
            passed=n_null == 0,
            failed_rows=n_null,
            details=f"{n_null} null values in {col}",
        ))
    return results


def check_unique(df: pd.DataFrame, table_name: str, columns: list[str]) -> DQResult:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        return DQResult(f"unique:{columns}", table_name, False, len(df), f"missing columns {missing}")
    n_dupes = int(df.duplicated(subset=columns).sum())
    return DQResult(
        check_name=f"unique:{'+'.join(columns)}",
        table_name=table_name,
        passed=n_dupes == 0,
        failed_rows=n_dupes,
        details=f"{n_dupes} duplicate rows on {columns}",
    )


def check_row_count_min(df: pd.DataFrame, table_name: str, minimum: int) -> DQResult:
    n = len(df)
    return DQResult(
        check_name="row_count_min",
        table_name=table_name,
        passed=n >= minimum,
        failed_rows=0 if n >= minimum else minimum - n,
        details=f"{n} rows loaded, expected >= {minimum}",
    )


def check_values_in_set(df: pd.DataFrame, table_name: str, column: str, allowed: set) -> DQResult:
    if column not in df.columns:
        return DQResult(f"accepted_values:{column}", table_name, False, len(df), f"column {column} missing")
    bad_mask = ~df[column].isin(allowed) & df[column].notna()
    n_bad = int(bad_mask.sum())
    return DQResult(
        check_name=f"accepted_values:{column}",
        table_name=table_name,
        passed=n_bad == 0,
        failed_rows=n_bad,
        details=f"{n_bad} rows with {column} outside {sorted(allowed)}",
    )


def run_checks(checks: list[Callable[[], list[DQResult] | DQResult]]) -> list[DQResult]:
    """Runs a list of zero-arg callables (closures over df/table_name/etc.)
    and flattens their results into one list."""
    results: list[DQResult] = []
    for check in checks:
        outcome = check()
        if isinstance(outcome, list):
            results.extend(outcome)
        else:
            results.append(outcome)
    return results


def any_failed(results: list[DQResult]) -> bool:
    return any(not r.passed for r in results)