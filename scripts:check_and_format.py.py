#!/usr/bin/env python3
"""
Simple data-check + lightweight formatting helper for CSV tables.

What it does (safe & non-destructive):
- For each CSV path you list, it reads either:
  - the full file if file size < large_threshold_bytes, or
  - a sample (nrows) otherwise (to avoid OOM).
- Reports per-column: dtype (pandas), non-null count (in the loaded portion),
  number of distinct values, and heuristics:
    - datetime candidate: tries pd.to_datetime and reports parse failures
    - numeric candidate: tries pd.to_numeric and reports non-numeric entries
    - id candidate (subject_id/hadm_id/icustay_id/row_id): checks integer-ness
- Prints a human-readable summary and writes data_quality_summary.json.

Usage:
  python scripts/check_and_format.py
  # optional args:
  python scripts/check_and_format.py --sample 2000 --large-threshold-mb 150

Requirements:
  pip install pandas
"""
import os
import json
import argparse
from typing import Dict, Any, List
import pandas as pd

# Edit this list if your filenames differ / are in a subfolder
CSV_FILES = [
    "ADMISSIONS.csv", "CALLOUT.csv", "CAREGIVERS.csv", "CHARTEVENTS.csv",
    "CPTEVENTS.csv", "D_CPT.csv", "D_ICD_DIAGNOSES.csv", "D_ICD_PROCEDURES.csv",
    "D_ITEMS.csv", "D_LABITEMS.csv", "DATETIMEEVENTS.csv", "DIAGNOSES_ICD.csv",
    "DRGCODES.csv", "INPUTEVENTS_CV.csv", "INPUTEVENTS_MV.csv", "MICROBIOLOGYEVENTS.csv",
    "NOTEEVENTS.csv", "OUTPUTEVENTS.csv", "PATIENTS.csv", "PRESCRIPTIONS.csv",
    "PROCEDUREEVENTS_MV.csv", "PROCEDURES_ICD.csv", "SERVICES.csv", "TRANSFERS.csv"
]

ID_COLUMNS = {"SUBJECT_ID", "HADM_ID", "ICUSTAY_ID", "ROW_ID"}

def human_size(nbytes: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if nbytes < 1024.0:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024.0
    return f"{nbytes:.1f}PB"

def inspect_file(path: str, sample_rows: int, large_threshold_bytes: int) -> Dict[str, Any]:
    info: Dict[str, Any] = {"path": path, "exists": os.path.exists(path)}
    if not info["exists"]:
        return info

    size = os.path.getsize(path)
    info["size_bytes"] = size
    info["size_human"] = human_size(size)
    use_full = size <= large_threshold_bytes

    read_kwargs = {"low_memory": False}
    # For safety, never auto-parse dates here; we detect candidates and test explicitly.
    try:
        if use_full:
            df = pd.read_csv(path, **read_kwargs)
            info["loaded_rows"] = len(df)
            info["sampled"] = False
        else:
            df = pd.read_csv(path, nrows=sample_rows, **read_kwargs)
            info["loaded_rows"] = len(df)
            info["sampled"] = True
            info["note"] = f"file > {human_size(large_threshold_bytes)}; inspected first {sample_rows} rows"
    except Exception as e:
        info["error"] = f"read_csv failed: {repr(e)}"
        return info

    # normalize column names to uppercase to compare with MIMIC conventions
    df.columns = [c.upper() for c in df.columns]

    cols = list(df.columns)
    info["n_columns"] = len(cols)
    info["columns"] = cols

    col_summaries: Dict[str, Dict[str, Any]] = {}
    for c in cols:
        series = df[c]
        s_info: Dict[str, Any] = {}
        s_info["dtype_sample"] = str(series.dtype)
        s_info["non_null"] = int(series.notna().sum())
        s_info["nulls"] = int(series.isna().sum())
        s_info["n_unique"] = int(series.nunique(dropna=True))
        # candidate datetime if column name contains time/date or common patterns
        name_lower = c.lower()
        is_time_candidate = any(x in name_lower for x in ("time","date","dt","admit","intime","outtime","charttime","chartdate"))
        s_info["datetime_candidate"] = bool(is_time_candidate)
        if is_time_candidate:
            try:
                parsed = pd.to_datetime(series, errors="coerce", infer_datetime_format=True)
                parsed_na = parsed.isna().sum()
                s_info["datetime_parse_na"] = int(parsed_na)
                # parsing issues = parsed_na - original nulls (could be negative if original had nulls)
                parse_issues = max(0, int(parsed_na - s_info["nulls"]))
                s_info["datetime_parse_issues"] = parse_issues
                if parse_issues > 0:
                    # collect up to 5 distinct problematic strings for inspection
                    bad = series[parsed.isna() & series.notna()].astype(str)
                    s_info["datetime_parse_samples"] = bad.unique()[:5].tolist()
            except Exception as e:
                s_info["datetime_parse_error"] = repr(e)

        # numeric candidate: try coerce
        is_obj_or_number = series.dtype == "object" or pd.api.types.is_numeric_dtype(series)
        if is_obj_or_number:
            coerced = pd.to_numeric(series, errors="coerce")
            non_numeric = int(coerced.isna().sum() - series.isna().sum())
            s_info["non_numeric_entries"] = max(0, non_numeric)
            if s_info["non_numeric_entries"] > 0:
                bad_vals = series[coerced.isna() & series.notna()].astype(str)
                s_info["non_numeric_samples"] = bad_vals.unique()[:5].tolist()

        # id-like checks
        if c in ID_COLUMNS:
            coerced_id = pd.to_numeric(series, errors="coerce")
            # count values that are non-integer (after dropping NA)
            non_integer = int(((coerced_id.dropna() % 1) != 0).sum())
            na_ids = int(coerced_id.isna().sum())
            s_info["id_non_integer"] = non_integer
            s_info["id_na_in_sample"] = na_ids
            if non_integer > 0:
                s_info["id_non_integer_samples"] = series[coerced_id.dropna() % 1 != 0].astype(str).unique()[:5].tolist()

        col_summaries[c] = s_info

    info["columns_summary"] = col_summaries
    return info

def main(sample_rows: int, large_threshold_mb: int, files: List[str]):
    large_threshold_bytes = int(large_threshold_mb * 1024 * 1024)
    results = {}
    for path in files:
        print(f"Inspecting {path} ...", end=" ")
        res = inspect_file(path, sample_rows=sample_rows, large_threshold_bytes=large_threshold_bytes)
        results[path] = res
        if not res.get("exists"):
            print("MISSING")
            continue
        print(f"OK (size {res['size_human']}, loaded_rows={res.get('loaded_rows')}, sampled={res.get('sampled', False)})")
        # short print of issues if any
        issues = []
        for col, s in res.get("columns_summary", {}).items():
            if s.get("datetime_parse_issues", 0) > 0:
                issues.append(f"{col}: datetime parse issues={s['datetime_parse_issues']}")
            if s.get("non_numeric_entries", 0) > 0:
                issues.append(f"{col}: non-numeric_entries={s['non_numeric_entries']}")
            if s.get("id_non_integer", 0) > 0:
                issues.append(f"{col}: id_non_integer={s['id_non_integer']}")
            if s.get("nulls", 0) == res.get("loaded_rows"):
                issues.append(f"{col}: ALL NULL in sample")
        if issues:
            print("  Issues:")
            for it in issues[:6]:
                print("   -", it)
        else:
            print("  No obvious issues in sample.")
    # write results json
    out_path = "data_quality_summary.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSummary written to {out_path}.")
    print("Open that JSON or paste relevant parts here if you want help interpreting.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=2000, help="rows to sample for large files (default 2000)")
    ap.add_argument("--large-threshold-mb", type=int, default=100, help="treat files larger than this as large and only sample (MB)")
    ap.add_argument("--files", nargs="*", default=CSV_FILES, help="list of CSV files to inspect")
    args = ap.parse_args()
    main(sample_rows=args.sample, large_threshold_mb=args.large_threshold_mb, files=args.files)