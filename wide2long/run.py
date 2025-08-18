import argparse
from pathlib import Path
from wide2long.core import convert, load_mapping, load_data, save_data


def run():
    ap = argparse.ArgumentParser(description="A wide data to Semi-long (block-diagonal) data converter")
    ap.add_argument("--input"       , required= True  , type=Path, help="Input wide data (.csv or .parquet)")
    ap.add_argument("--output"      , required= True  , type=Path, help="Output data (.csv or .parquet)")
    ap.add_argument("--mapping"     , required= True  , type=Path, help="Mapping file (.csv or .json)")
    ap.add_argument("--id-cols"     , required= True  , nargs="+", help="ID columns to keep")
    ap.add_argument("--csv-sep"     , default = ","   ,            help="CSV delimiter for reading input data (default ',')")
    ap.add_argument("--no-validate" , action="store_true",         help="Allow last mapping to override on conflicts")
    args = ap.parse_args()

    df = load_data(args.input, csv_sep=args.csv_sep)
    selections = load_mapping(args.mapping)

    try:
        out = convert(
            df=df,
            id_cols=args.id_cols,
            mapping=selections,
            validate=not args.no_validate,
        )
    except Exception as e:
        ap.error(str(e))

    save_data(out, args.output)
    print(f"Saved {args.output}.")


if __name__ == "__main__":
    run()