import pandas as pd
import sqlite3

conn = sqlite3.connect('C:/Users/David/Downloads/dbs_2/electricity.sqlite')
conn.execute('ATTACH "nz_v3_unconstrained.sqlite" as cam')
curs = conn.cursor()

# NOTES
# NL -> NLLAB
# PE -> PEI
# No WND 14/15

# Just to check which tables are relevant
# tables = [t[0] for t in curs.execute('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name;').fetchall()]
# ws_tables = []
# for table in tables:
#     cols = [c[1] for c in conn.execute(f'PRAGMA table_info({table});').fetchall()]
#     if 'tech' in cols:
#         data = curs.execute(f'SELECT * FROM {table} WHERE tech LIKE "E_SOL_PV-NEW%" OR tech LIKE "E_WND_ON-NEW%" LIMIT 1').fetchone()
#         if data:
#             ws_tables.append(table)

# CapacityCredit
df = pd.read_sql_query(
    "SELECT region, period, tech, vintage, credit, notes "
    "FROM cam.CapacityCredit "
    "WHERE tech LIKE '%NEW-%' "
    "AND tech NOT like '%14' "
    "AND tech NOT like '%15' ",
    conn
)
df.loc[df['region'] == 'NL', 'region'] = 'NLLAB'
df.loc[df['region'] == 'PE', 'region'] = 'PEI'
df['data_id'] = [f"ELCHR{r}001" for r in df['region']]
data = df.to_numpy()
conn.executemany(
    'REPLACE INTO CapacityCredit(region, period, tech, vintage, credit, notes, data_id) '
    'VALUES(?,?,?,?,?,?,?)',
    data
)

# MaxCapacity
df = pd.read_sql_query(
    "SELECT region, period, tech, max_cap/1000, notes "
    "FROM cam.MaxCapacity "
    "WHERE tech LIKE '%NEW-%' "
    "AND tech NOT like '%14' "
    "AND tech NOT like '%15' ",
    conn
)
df.loc[df['region'] == 'NL', 'region'] = 'NLLAB'
df.loc[df['region'] == 'PE', 'region'] = 'PEI'
df['data_id'] = [f"ELCHR{r}001" for r in df['region']]
data = df.to_numpy()
conn.executemany(
    'REPLACE INTO LimitCapacity(region, period, tech_or_group, operator, capacity, notes, data_id) '
    'VALUES(?,?,?,"le",?,?,?)',
    data
)

# CostFixed
df = pd.read_sql_query(
    "SELECT region, period, tech, vintage, cost/1000, notes "
    "FROM cam.CostFixed "
    "WHERE tech LIKE '%NEW-%' "
    "AND tech NOT like '%14' "
    "AND tech NOT like '%15' ",
    conn
)
df.loc[df['region'] == 'NL', 'region'] = 'NLLAB'
df.loc[df['region'] == 'PE', 'region'] = 'PEI'
df['data_id'] = [f"ELCHR{r}001" for r in df['region']]
data = df.to_numpy()
conn.executemany(
    'REPLACE INTO CostFixed(region, period, tech, vintage, cost, notes, data_id) '
    'VALUES(?,?,?,?,?,?,?)',
    data
)

# CostInvest
df = pd.read_sql_query(
    "SELECT region, tech, vintage, cost/1000, notes "
    "FROM cam.CostInvest "
    "WHERE tech LIKE '%NEW-%' "
    "AND tech NOT like '%14' "
    "AND tech NOT like '%15' ",
    conn
)
df.loc[df['region'] == 'NL', 'region'] = 'NLLAB'
df.loc[df['region'] == 'PE', 'region'] = 'PEI'
df['data_id'] = [f"ELCHR{r}001" for r in df['region']]
data = df.to_numpy()
conn.executemany(
    'REPLACE INTO CostInvest(region, tech, vintage, cost, notes, data_id) '
    'VALUES(?,?,?,?,?,?)',
    data
)

# CapacityFactorProcess
df = pd.read_sql_query(
    "SELECT region, season, tod, tech, vintage, factor, notes "
    "FROM cam.CapacityFactorProcess "
    "WHERE tech LIKE '%NEW-%' "
    "AND tech NOT like '%14' "
    "AND tech NOT like '%15' ",
    conn
)
df.loc[df['region'] == 'NL', 'region'] = 'NLLAB'
df.loc[df['region'] == 'PE', 'region'] = 'PEI'
df['data_id'] = [f"ELCHR{r}001" for r in df['region']]
for period in range(2025,2055,5):
    data = df.to_numpy()
    conn.executemany(
        'REPLACE INTO CapacityFactorProcess(region, period, season, tod, tech, vintage, factor, notes, data_id) '
        f'VALUES(?,{period},?,?,?,?,?,?,?)',
        data
    )
curs.execute('DELETE FROM CapacityFactorProcess WHERE period < vintage or vintage + 30 <= period')

conn.commit()
conn.execute('VACUUM;')
conn.commit()
conn.close()