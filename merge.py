"""
A simple script to merge sqlite modules into the CANOE dataset
"""

import sqlite3
import os

this_dir = os.path.dirname(os.path.realpath(__file__))
schema = os.path.join(this_dir, 'canoe_dataset_schema.sql')
main_db = 'canoe_dataset.sqlite'


def init_connection() -> sqlite3.Connection:

    if not os.path.isfile(main_db):
        conn = sqlite3.connect(main_db)
        conn.executescript(open(schema, 'r').read())
    else:
        conn = sqlite3.connect(main_db)

    return conn


skip_tables = [
    'CommodityLabel',
    'TechnologyLabel',
    'DataSourceLabel',
    'SeasonLabel',
    'SectorLabel',
    'TimeSeason',
    'TimeSegmentFraction',
    'TimeSeasonSequential',
    'TimeOfDay'
]


def merge(merge_db: str):

    print(f'\nAttempting to merge {merge_db} into {main_db}...\n')

    if not os.path.isfile(merge_db):
        print(f'{merge_db} not found. Aborting.')
        return

    conn = sqlite3.connect(merge_db)
    curs = conn.cursor()

    merge_tables = [
        t[0] for t in curs.execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()
        if t[0] not in skip_tables
    ]

    abort = False

    print('Performing some checks on the input database...')

    # First we check that everything has a data ID
    for table in merge_tables:
        cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
        if 'data_id' in cols and table != 'DataSet':
            missing = curs.execute(f'SELECT * FROM {table} WHERE data_id IS NULL OR data_id == ""').fetchall()
            if missing:
                abort = True
                for row in missing: print(row)
                print(f'The above data in table {table} did not have a data ID!\n')
    
    # Next we check that all data IDs have a data set
    data_ids = set()
    for table in merge_tables:
        cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
        if 'data_id' in cols and table != 'DataSet':
            data_ids = data_ids.union([d[0] for d in curs.execute(f'SELECT data_id FROM {table} WHERE data_id IS NOT NULL').fetchall()])
    missing = []
    for data_id in data_ids:
        row = curs.execute(f'SELECT * FROM DataSet WHERE data_id == "{data_id}"').fetchall()
        if len(row) != 1:
            missing.append(data_id)
    if missing:
        abort = True
        print(missing)
        print(f'The above data IDs were not defined in the DataSet table!\n')

    # Finally, we check that the DataSet table has been filled out
    must_fill = ['data_id', 'label', 'version', 'description', 'status', 'author', 'date', 'changelog']
    rows = []
    for col in must_fill:
        missing = curs.execute(f'SELECT * FROM DataSet WHERE {col} IS NULL OR {col} == ""')
        rows.extend(missing)
    if rows:
        abort = True
        for row in rows: print(row)
        print(f'The above data IDs in DataSet were not fully filled out. Must fill out all of {must_fill}!\n')

    # Abort if there were errors
    if abort:
        print('Aborting.')
        conn.close()
        return

    print('All checks passed! \nBeginning merge...')

    conn.close()
    conn = init_connection()
    curs = conn.cursor()
    curs.execute('PRAGMA FOREIGN_KEYS = 0;')
    conn.execute(f'ATTACH "{merge_db}" AS merge_db')

    for i, table in enumerate(merge_tables):
        print(f'\rTransferring {table}, table {i}/{len(merge_tables)-1}...         ', end='')
        cols = [c[1] for c in curs.execute(f'PRAGMA table_info({table});')]
        if 'data_id' in cols:
            try:
                curs.execute(f'INSERT INTO {table} SELECT * FROM merge_db.{table}')
            except sqlite3.IntegrityError as e:
                print(f'Failed to merge data for table {table}. {e}')
                abort=True
        else:
            curs.execute(f'INSERT OR IGNORE INTO {table} SELECT * FROM merge_db.{table}')
    if abort:
        print('Aborting.')
        conn.close()
        return
    
    # These tables are just for foreign keys in the core dataset... very annoying but necessary
    # as we may have multiple variants of each of these
    curs.execute('INSERT OR IGNORE INTO CommodityLabel(commodity) SELECT name FROM Commodity')
    curs.execute('INSERT OR IGNORE INTO TechnologyLabel(tech) SELECT tech FROM Technology')
    curs.execute('INSERT OR IGNORE INTO SectorLabel(sector) SELECT sector FROM Technology')
    curs.execute('INSERT OR IGNORE INTO DataSourceLabel(source_id) SELECT source_id FROM DataSource')
    
    print('\nChecking foreign key integrity...')
    curs.execute('PRAGMA FOREIGN_KEYS = 1;')
    try:
        data = conn.execute('PRAGMA FOREIGN_KEY_CHECK;').fetchall()
        if data:
            print('(Table, Row ID, Reference Table, (fkid) )')
            for row in data:
                print(f'{row}')
            print(f'The above foreign keys failed to validate after merging {merge_db}.')
            abort = True
    except sqlite3.OperationalError as e:
        print(f'Foreign keys failed afer merging {merge_db}.')
        print(e)
        abort = True
    if abort:
        print('Aborting.')
        conn.close()
        return
    
    print('Foreign key integrity checks passed! \nCommitting changes...')

    conn.commit()
    conn.close()

    print('Finished!')


if __name__ == "__main__":

    cd = input("Set your database directory (or press Enter to use the current directory): ")
    if cd:
        os.chdir(cd)

    while True:
        db = input('Which database would you like to merge in? (e.g., electricity.sqlite): ')
        merge(db)