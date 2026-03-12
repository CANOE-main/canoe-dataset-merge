"""
Final infeasibility tests
"""

import sqlite3
import pandas as pd

conn = sqlite3.connect("canoe_dataset.sqlite")

df = pd.read_sql_query('SELECT * FROM LimitTechInputSplitAnnual', conn)
df = df.groupby(['region','period','tech','operator'])['proportion'].sum()
print(df.loc[(df<1)&(df.index.get_level_values('operator')=='le')]-1)
print(df.loc[(df>1)&(df.index.get_level_values('operator')=='ge')]-1)

df = pd.read_sql_query('SELECT * FROM LimitAnnualCapacityFactor WHERE output_comm IN (SELECT DISTINCT demand_name FROM DemandSpecificDistribution)', conn)
df_dsd = pd.read_sql_query('SELECT * FROM DemandSpecificDistribution', conn).groupby(['region','period','demand_name'])['dsd']
max_acf = (df_dsd.mean() / df_dsd.max()).to_dict()
df['max_acf'] = [max_acf[tuple(rpo)] for rpo in df[['region','period','output_comm']].values]
print(df.loc[(df['operator'] == 'ge') & (df['factor'] > df['max_acf'])])

conn.close()