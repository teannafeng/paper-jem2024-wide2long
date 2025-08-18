import json
import pandas as pd
from wide2long.core import load_mapping, convert

def test_json_key_value_style(tmp_path):
    df = pd.DataFrame({
        "school": ["A"],
        "person": [1],
        "pre_i1": [1],
        "pst_i1": [0],
        "l1_k1" : [0],
        "l1_k2" : [1],
        "l2_k1" : [0]
    })

    # key–value style mapping
    mapping = {
        "pre_i1": ["i1", "pre"],
        "pst_i1": ["i1", "pst"],
        "l1_k1" : ["l1", "k1"],
        "l1_k2" : ["l1", "k2"],
        "l2_k1" : ["l2", "k1"]
    }

    mpath = tmp_path / "json_key_value_mapping.json"
    mpath.write_text(json.dumps(mapping), encoding="utf-8")

    selections = load_mapping(mpath)
    out = convert(df, id_cols=["school", "person"], mapping=selections)

    # Check columns
    assert set(out.columns) == {"school", "person", "element", "pre", "pst", "k1", "k2"}

    # Check element ids
    assert set(out["element"]) == {"i1", "l1", "l2"}

    # i1 row: pre/pst filled, k* empty
    row_i1 = out[out["element"] == "i1"].iloc[0]
    assert row_i1["pre"] == 1 and row_i1["pst"] == 0
    assert pd.isna(row_i1["k1"]) and pd.isna(row_i1["k2"])

    # l1 row: k1/k2 filled, pre/pst empty
    row_l1 = out[out["element"] == "l1"].iloc[0]
    assert row_l1["k1"] == 0 and row_l1["k2"] == 1
    assert pd.isna(row_l1["pre"]) and pd.isna(row_l1["pst"])

    # l2 row: k1 filled, k2 missing, pre/pst empty
    row_l2 = out[out["element"] == "l2"].iloc[0]
    assert row_l2["k1"] == 0 and pd.isna(row_l2["k2"])
    assert pd.isna(row_l2["pre"]) and pd.isna(row_l2["pst"])