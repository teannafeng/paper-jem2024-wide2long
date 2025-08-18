import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Iterable

ColMap = Dict[
    str,     # column name in wide data
    Tuple[
        str, # element label in the processed table
        str  # variable column name in the processed table
    ] # 
]
REQUIRED_KEYS = {"source_col", "element_id", "variable_col"}

def convert(
    df: pd.DataFrame,
    id_cols: List[str],
    mapping: Iterable[ColMap],
    *,
    validate: bool = True
) -> pd.DataFrame:
    """
    Reshape a DataFrame from wide → long (block-diagonal style) using 
    explicit user selections.

    Args:
        df:         Input wide data.
        id_cols:    Columns to use as identifiers (e.g., school, person)
        mapping:    List of dictionaries for mapping columns in the 
                    wide data to processed data
        validate:   If True, raise an error if the same wide-data column is 
                    mapped to different (element_id, variable_col) pairs.
    """
    
    # Merge column selections
    colmap: ColMap = {}
    for m in mapping:
        for k, v in m.items():
            if validate and k in colmap and colmap[k] != v:
                raise ValueError(f"Column '{k}' was mapped to different targets: {colmap[k]} vs {v}.")
            colmap[k] = v
    if not colmap:
        raise ValueError("No columns were selected.")

    # Do sanity checks
    if validate:
        missing = [c for c in colmap if c not in df.columns]
        if missing:
            raise KeyError(f"Selected columns not in DataFrame: {missing}.")
        for c in id_cols:
            if c not in df.columns:
                raise KeyError(f"id column '{c}' not in DataFrame.")

    # Create helper map
    helper_map = {src: f"__H__{elt}|{tgt}" for src, (elt, tgt) in colmap.items()}
    df_helper = df.rename(columns=helper_map)

    # Pivot data from wide to long
    df_helper_long = df_helper.melt(
        id_vars=id_cols,
        value_vars=list(helper_map.values()),
        var_name="__h",
        value_name="value"
    )

    # Add 'element' col + cols for variables
    parts = (
        df_helper_long["__h"]
        .str.replace("^__H__", "", regex=True)
        .str.split("|", n=1, expand=True)
        .rename(columns={0: "element", 1: "__col"})
    )
    df_helper_long = pd.concat([df_helper_long[id_cols], parts, df_helper_long["value"]], axis=1)

    # Pivot again
    out = (df_helper_long
           .pivot_table(values="value",
                        index=id_cols + ["element"],
                        columns="__col",
                        aggfunc="first")
           .reset_index()
           .rename_axis(None, axis=1))

    return out


def _is_block_style_named(d: dict) -> bool:
    # {"block_a": [ {source_col, element_id, variable_col}, ... ], ...}
    return (
        isinstance(d, dict)
        and len(d) > 0
        and all(
            isinstance(v, list) and
            all(isinstance(item, dict) and REQUIRED_KEYS.issubset(item) for item in v)
            for v in d.values()
        )
    )

def _is_key_value_style(d: dict) -> bool:
    # {"pre_i1": ["i1","pre"], "pst_i1": ["i1","pst"], ...}
    return (
        isinstance(d, dict)
        and len(d) > 0
        and all(
            isinstance(v, (list, tuple)) and len(v) == 2
            for v in d.values()
        )
    )


def load_mapping(mapping_path: Path | str):
    """
    Load a mapping file that describes how to reshape wide → long.

    Support one of the following specification formats:
      1) CSV with columns: source_col, element_id, variable_col
      2) JSON:
        - Block style (named blocks):
            {
                "block_a": [ {"source_col": "...", "element_id": "...", "variable_col": "..."}, ... ],
                "block_b": [ ... ]
            }
        - Key-value style:
            { "source_col": ["element_id", "variable_col"], ... }
    """
    if isinstance(mapping_path, str):
        mapping_path = Path(mapping_path)

    if mapping_path.suffix.lower() == ".csv":
        mdf = pd.read_csv(mapping_path, encoding="utf-8")
        required = set(REQUIRED_KEYS)
        if not required.issubset(mdf.columns):
            raise ValueError(f"Mapping CSV must have columns: {sorted(required)}.")
        # strip whitespace from the three columns
        for col in ["source_col", "element_id", "variable_col"]:
            mdf[col] = mdf[col].astype(str).str.strip()
        return [{row.source_col: (row.element_id, row.variable_col)} for _, row in mdf.iterrows()]


    if mapping_path.suffix.lower() == ".json":
        _mjson = mapping_path.read_text(encoding="utf-8")
        mjson = json.loads(_mjson)
        selections: List[ColMap] = []

        # (A) Block style with named blocks: content must be a list of dicts
        if _is_block_style_named(mjson):
            for block_name, block in mjson.items():
                for row in block:
                    src = str(row["source_col"]).strip()
                    ele = str(row["element_id"]).strip()
                    var = str(row["variable_col"]).strip()
                    selections.append({src: (ele, var)})
            return selections

        # (B) Key–value style: { "source_col": ["element_id","variable_col"], ... }
        if _is_key_value_style(mjson):
            for k, v in mjson.items():
                if not (isinstance(v, (list, tuple)) and len(v) == 2):
                    raise ValueError("Key–value JSON mapping values must be [element_id, variable_col]")
                src = str(k).strip()
                ele = str(v[0]).strip()
                var = str(v[1]).strip()
                selections.append({src: (ele, var)})
            return selections
        
        raise ValueError(
            "Unrecognized JSON mapping format. Expected either named blocks "
            '({"block_a": [ {...}, ... ]}) '
            'or key-value style ({"pre_i1": ["i1","pre"], ...}).'
        )

    raise ValueError("Mapping must be .csv or .json")


def _check_row_keys(row: dict, block_name: str) -> None:
    needed = {"source_col", "element_id", "variable_col"}
    if not needed.issubset(row):
        missing = sorted(needed - set(row))
        raise ValueError(f"Block '{block_name}' has rows missing keys: {missing}")


def load_data(path: Path, csv_sep: str = ",") -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, encoding="utf-8", sep=csv_sep)
        return df
    if path.suffix.lower() in [".parquet", ".pq"]:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input format: {path.suffix} (use .csv or .parquet).")


def save_data(df: pd.DataFrame, path: Path):
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    elif path.suffix.lower() in [".parquet", ".pq"]:
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported output format: {path.suffix} (use .csv or .parquet).")
    