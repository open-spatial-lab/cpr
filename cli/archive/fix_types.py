# %%
import pandas as pd
import numpy as np
# remove col limit
pd.set_option('display.max_columns', None)
# %%
tract = pd.read_parquet('../data/community vars/ca-tract.parquet')
# %%
# use loc to index rows where "Median Household Income (In 2021 Inflation Adjusted Dollars)" == np.nan
tract.loc[tract["Median Household Income (In 2021 Inflation Adjusted Dollars)"].isna()]["Median Household Income (In 2021 Inflation Adjusted Dollars)"] = None

# %%
values = tract["Median Household Income (In 2021 Inflation Adjusted Dollars)"]
for value in values:
  if value is np.nan:
    print("None")
# %%
