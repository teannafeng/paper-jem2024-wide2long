import json
import pandas as pd
from wide2long.core import load_mapping, convert

def test_json_block_style(tmp_path):
    df = pd.DataFrame({
        "school": [1],
        "person": [1],
        "pre_i1": [0],
        "pst_i1": [1],
        "j1_k1" : [2],
        "j2_k1" : [3]
    })

    mapping = {
        "block_a": [
            {"source_col": "pre_i1", "element_id": "i1", "variable_col": "pre"},
            {"source_col": "pst_i1", "element_id": "i1", "variable_col": "pst"}
        ],
        "block_b": [
            {"source_col": "j1_k1", "element_id": "j1", "variable_col": "k1"},
            {"source_col": "j2_k1", "element_id": "j2", "variable_col": "k1"}
        ]
    }
    
    mpath = tmp_path / "json_block_mapping.json"
    mpath.write_text(json.dumps(mapping), encoding="utf-8")

    selections = load_mapping(mpath)
    out = convert(df, id_cols=["school", "person"], mapping=selections)

    # Columns present
    assert set(out.columns) == {"school", "person", "element", "pre", "pst", "k1"}

    # Elements present
    assert set(out["element"]) == {"i1", "j1", "j2"}

    # Values landed correctly
    row_i1 = out[out["element"] == "i1"].iloc[0]
    assert row_i1["pre"] == 0 and row_i1["pst"] == 1

    row_j1 = out[out["element"] == "j1"].iloc[0]
    row_j2 = out[out["element"] == "j2"].iloc[0]
    assert row_j1["k1"] == 2
    assert row_j2["k1"] == 3