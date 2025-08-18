import pandas as pd
from wide2long.core import load_mapping, convert

def test_csv_mapping(tmp_path):
    df = pd.DataFrame({
        "school": ["A"],
        "person": [1],
        "pre_i1": [1],
        "pst_i1": [0],
        "l1_k1" : [0],
        "l1_k2" : [0],
        "l2_k1" : [0],
    })

    # CSV mapping text
    csv_text = """source_col,element_id,variable_col
    pre_i1,i1,pre
    pst_i1,i1,pst
    l1_k1,l1,k1
    l1_k2,l1,k2
    l2_k1,l2,k1
    """
    mpath = tmp_path / "csv_mapping.csv"
    mpath.write_text(csv_text, encoding="utf-8")

    selections = load_mapping(mpath)
    out = convert(df, id_cols=["school", "person"], mapping=selections)

    # Columns present
    assert set(out.columns) == {"school", "person", "element", "pre", "pst", "k1", "k2"}

    # Elements present
    assert set(out["element"]) == {"i1", "l1", "l2"}

    # i1 row: pre/pst filled, k* empty
    row_i1 = out[out["element"] == "i1"].iloc[0]
    assert row_i1["pre"] == 1 and row_i1["pst"] == 0
    assert pd.isna(row_i1["k1"]) and pd.isna(row_i1["k2"])

    # l1 row: k1/k2 filled
    row_l1 = out[out["element"] == "l1"].iloc[0]
    assert row_l1["k1"] == 0 and row_l1["k2"] == 0
    assert pd.isna(row_l1["pre"]) and pd.isna(row_l1["pst"])

    # l2 row: only k1 filled
    row_l2 = out[out["element"] == "l2"].iloc[0]
    assert row_l2["k1"] == 0
    assert pd.isna(row_l2["k2"]) and pd.isna(row_l2["pre"]) and pd.isna(row_l2["pst"])
