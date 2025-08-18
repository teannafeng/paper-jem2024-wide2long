# Wide2Long: A Wide to Long Block-Diagonal Data Converter

This package implements the **wide → semi-long** data transformation described in [Feng & Cai (2024)](https://onlinelibrary.wiley.com/doi/full/10.1111/jedm.12396).

In our paper, this corresponds to the data re-structuring from Table 4 to Table 5 (without the school dummies created). In that case, we work with **two data blocks**: one for assessment item data and another for gameplay indicator data.

The package also supports **more than two data blocks or sources**, as long as the mapping specifies how each data block (elements and variables) should be re-structured into the semi-long format.

## Installation

```bash
git clone https://github.com/teannafeng/paper-jem2024-wide2long.git
cd paper-jem2024-wide2long
pip install -r requirements.txt # better to do this in a virtual environment
```

## Usage

### Command line

```bash
python -m wide2long.run --input ./example/example_wide_data.csv --mapping ./example/mapping.csv --id-cols school person --output ./example/output.csv
```

### Python script

```python
import pandas as pd
from wide2long.core import convert, load_mapping

# Load input wide data
df = pd.read_csv("./example/example_wide_data.csv")

# Load column mapping
mapping = load_mapping("./example/mapping.csv")

# Convert data from wide to semi-long format
out = convert(df, id_cols=["school", "person"], mapping=mapping)

# Save output data
out.to_csv("./example/output.csv", index=False)
```

### Mapping formats

Mappings of the wide-data columns to semi-long-data columns can be defined in CSV or JSON.

#### CSV mapping example

| source\_col | element\_id | variable\_col |
| ----------- | ----------- | ------------- |
| pre\_i1     | i1          | pre           |
| pst\_i1     | i1          | pst           |
| j1\_k1      | j1          | k1            |
| j2\_k1      | j2          | k1            |

#### JSON mapping example

Block style:

```json
{
  "block_a": [
    {"source_col": "pre_i1", "element_id": "i1", "variable_col": "pre"},
    {"source_col": "pst_i1", "element_id": "i1", "variable_col": "pst"},
  ],
  "block_b": [
    {"source_col": "j1_k1",  "element_id": "j1", "variable_col": "k1"},
    {"source_col": "j2_k1",  "element_id": "j2", "variable_col": "k1"}
  ]
}
```

Key–value style:

```json
{
  "pre_i1": ["i1", "pre"],
  "pst_i1": ["i1", "pst"],
  "j1_k1":  ["j1", "k1"],
  "j2_k1":  ["j2", "k1"]
}
```
