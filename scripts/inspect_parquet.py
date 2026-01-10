import sys
import pandas as pd

p = sys.argv[1]
df = pd.read_parquet(p)
print('COLUMNS:', df.columns.tolist())
print(df.head(3).to_dict())
