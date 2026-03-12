import sqlite3
import pandas as pd
import numpy as np

thresh = 0.001

conn = sqlite3.connect('canoe_dataset.sqlite')
curs = conn.cursor()

"""
Clean up tiny existing capacities
"""
existing_rtv = set(curs.execute(f"SELECT region, tech, vintage FROM ExistingCapacity").fetchall())
remove_rtv = set(curs.execute(f"SELECT region, tech, vintage FROM ExistingCapacity WHERE capacity < {thresh}").fetchall())
new_rtv = set(curs.execute(f"SELECT region, tech, vintage FROM Efficiency WHERE vintage >= 2025").fetchall())

old_rtv = existing_rtv.union(new_rtv)
remaining_rtv = old_rtv.difference(remove_rtv)

rt = {rtv[0:2] for rtv in old_rtv}
remaining_rt = {rtv[0:2] for rtv in remaining_rtv}
remove_rt = rt - remaining_rt

t = {rtv[0] for rtv in old_rtv}
remaining_t = {rtv[0] for rtv in remaining_rtv}
remove_t = t - remaining_t

for rtv in remove_rtv:
    print(rtv)
for rt in remove_rt:
    print(rt)
for t in remove_t:
    print(t)

curs.execute(f"DELETE FROM ExistingCapacity WHERE capacity < {thresh}")

tables = [t[0] for t in curs.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()]

for table in tables:
    if table[0:6] == 'Output': continue
    cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
    if 'region' in cols and 'tech' in cols and 'vintage' in cols:
        for rtv in remove_rtv:
            cmd = f"DELETE FROM {table} WHERE region == '{rtv[0]}' and tech == '{rtv[1]}' and vintage == {rtv[2]}"
            curs.execute(cmd)
    elif 'region' in cols and 'tech' in cols:
        for rt in remove_rt:
            cmd = f"DELETE FROM {table} WHERE region == '{rt[0]}' and tech == '{rt[1]}'"
            curs.execute(cmd)
    elif 'tech' in cols:
        for t in remove_t:
            cmd = f"DELETE FROM {table} WHERE tech == '{t}'"
            curs.execute(cmd)

"""
Remove any region-tech pairs where nothing in this region uses its output
"""
continuing = True
while continuing:
    regions = [r[0] for r in curs.execute('SELECT region FROM Region').fetchall()]
    bad_rt = curs.execute(
        """
        SELECT region, tech
        FROM Efficiency
        WHERE output_comm NOT IN (SELECT name FROM Commodity WHERE flag == "d")
            AND (region, output_comm) NOT IN (
                SELECT region, input_comm FROM Efficiency
            )
        """
    ).fetchall()

    continuing = len(bad_rt) > 0

    print(f"Removing bad region-tech pairs: {bad_rt}")

    tables = [t[0] for t in curs.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()]

    for table in tables:
        for rt in bad_rt:
            cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
            if 'region' in cols and 'tech' in cols:
                curs.execute(f'DELETE FROM {table} WHERE region == "{rt[0]}" and tech == "{rt[1]}"')

    tech_remaining = {t[0] for t in curs.execute('SELECT DISTINCT tech FROM Efficiency').fetchall()}
    tech_before = {t[0] for t in curs.execute('SELECT DISTINCT tech FROM Technology').fetchall()}
    tech_gone = tech_before - tech_remaining

    print(f"Removing bad techs: {tech_gone}")

    for table in tables:
        for tech in tech_gone:
            cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
            if 'tech' in cols:
                curs.execute(f'DELETE FROM {table} WHERE tech == "{tech}"')

"""
Remove and region-tech-vintage combos (presumed lifetime = 5) where nothing consumes its output
in that period
"""
continuing = True
while continuing:
    # Need these
    time_all = curs.execute('SELECT period FROM TimePeriod').fetchall()
    time_all = [p[0] for p in time_all[0:-1]]

    # get lifetimes. Major headache but needs to be done
    lifetime_process = dict()
    data = curs.execute('SELECT region, tech, vintage FROM Efficiency').fetchall()
    for rtv in data:
        lifetime_process[rtv] = 40
    data = curs.execute('SELECT region, tech, lifetime FROM LifetimeTech').fetchall()
    for rtl in data:
        for v in time_all:
            lifetime_process[(*rtl[0:2], v)] = rtl[2]
    data = curs.execute('SELECT region, tech, vintage, lifetime FROM LifetimeProcess').fetchall()
    for rtvl in data:
        lifetime_process[rtvl[0:3]] = rtvl[3]

    # Get the efficiency table
    df_eff = pd.read_sql_query('SELECT * FROM Efficiency', conn)

    # Last period each process is active
    df_eff['last_period'] = df_eff['vintage'] + [int(lifetime_process[tuple(rtv)]) for rtv in df_eff[['region','tech','vintage']].values]
    df_eff['last_period'] = [min(2050,5*((p-1) // 5)) for p in df_eff['last_period']]

    # Last period each commodity is consumed in each region
    df_last_consume = df_eff.groupby(['region','input_comm'])['last_period'].max()
    demand_comms = [c[0] for c in curs.execute("SELECT name FROM Commodity WHERE flag == 'd'").fetchall()]
    df_eff = df_eff.loc[~df_eff['output_comm'].isin(demand_comms)]
    try:
        df_eff['last_consume'] = [df_last_consume.loc[tuple(ro)] for ro in df_eff[['region','output_comm']].values]
    except Exception as e:
        df_eff.to_csv('eff.csv')
        raise RuntimeError(e)
    df_last_consume.to_csv('last_consume.csv')

    # Remove any processes that are producing their output comm after anything is consuming it
    df_remove = df_eff.loc[df_eff['last_consume'] < df_eff['last_period']]
    bad_ritvo = df_remove[['region','input_comm','tech','vintage','output_comm']].values

    continuing = len(bad_ritvo) > 0

    print(f"Removing bad processes: {bad_ritvo}")
    
    for region, input_comm, tech, vintage, output_comm in bad_ritvo:
        curs.execute(
            f"""
            DELETE FROM Efficiency 
            WHERE 
            region == '{region}' AND 
            input_comm == '{input_comm}' AND 
            tech == '{tech}' AND 
            vintage == '{vintage}' AND 
            output_comm == '{output_comm}'
            """
        )
        curs.execute(
            f"""
            DELETE FROM CostVariable 
            WHERE 
            region == '{region}' AND 
            tech == '{tech}' AND 
            vintage == '{vintage}'
            """
        )
        curs.execute(
            f"""
            DELETE FROM CostFixed 
            WHERE 
            region == '{region}' AND 
            tech == '{tech}' AND 
            vintage == '{vintage}'
            """
        )
        curs.execute(
            f"""
            DELETE FROM EmissionActivity 
            WHERE 
            region == '{region}' AND 
            tech == '{tech}' AND 
            vintage == '{vintage}'
            """
        )

# Clean up unused commodities
curs.execute(
    'DELETE FROM Commodity '
    'WHERE flag != "e" '
    'AND name NOT IN (SELECT DISTINCT input_comm FROM Efficiency) '
    'AND name NOT IN (SELECT DISTINCT output_comm FROM Efficiency)'
)

conn.commit()
conn.execute('VACUUM;')
conn.commit()
conn.close()