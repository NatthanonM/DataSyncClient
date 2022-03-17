import sqlite3

conn = sqlite3.connect('my.db')
cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
cur.execute("PRAGMA auto_vacuum = FULL")
TABLE = 'data_record'
time_filename = "time.txt"


def sqlite_create_table():    # Create table
    statement = f'''CREATE TABLE IF NOT EXISTS`{TABLE}` (
        `uuid` CHAR(36),
        `author` VARCHAR(64),
        `message` VARCHAR(1024),
        `likes` unsigned INT(10),
        PRIMARY KEY (`uuid`)
    );'''
    cur.execute(statement)


def sqlite_delete_all():
    statement = f"DELETE FROM {TABLE}"
    cur.execute(statement)
    conn.commit()
    cur.execute('VACUUM')


def reset_data_sync():
    sqlite_delete_all()
    with open(time_filename, 'w') as f:
        f.write('1640970001\n')


sqlite_create_table()
reset_data_sync()

conn.close()
