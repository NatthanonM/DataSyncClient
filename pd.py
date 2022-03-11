import pickle
import pandas as pd
import sqlite3
import requests
import sqlite3
import calendar
import datetime
from urllib.parse import urljoin
import time

conn = sqlite3.connect('pd.db')
TABLE = 'data_record'
# endpoint = ".../api/messages/"
endpoint = ""
time_filename = "time.txt"


def sqlite_create_table(cur):    # Create table
    statement = f'''CREATE TABLE IF NOT EXISTS`{TABLE}` (
        `uuid` CHAR(36),
        `author` VARCHAR(64),
        `message` VARCHAR(1024),
        `likes` unsigned INT(10),
        PRIMARY KEY (`uuid`)
    );'''
    cur.execute(statement)


def sqlite_delete_all(cur):
    statement = f"DELETE FROM {TABLE}"
    cur.execute(statement)
    conn.commit()
    cur.execute('VACUUM')


def retrieve_commands_from_file(filename):
    a_file = open(filename, "rb")
    commands = pickle.load(a_file)
    date = datetime.datetime.utcnow()
    updated_at = calendar.timegm(date.utctimetuple())
    return (commands, updated_at)


def retrieve_commands():     # Retrieve command list from server
    while True:
        with open(time_filename) as f:
            latest = f.readline()
        # Declare present time
        date = datetime.datetime.utcnow()
        updated_at = calendar.timegm(date.utctimetuple())
        # Retrieve commands
        r = requests.get(urljoin(endpoint, latest))
        if r.status_code == 200:
            commands = r.json()
            break
    return (commands, updated_at)


def sqlite_print_all(cur):     # Query data and print it in csv format
    for row in cur.execute(f"SELECT * FROM {TABLE}"):
        print(*row, sep=",")


def pd_to_csv(df):             # Print dataframe in csv format
    output = df.to_csv(index=False, sep=',', header=False)
    print(output, end="")


def main():
    # commands, updated_at = retrieve_commands()
    commands, updated_at = retrieve_commands_from_file("commands.pkl")

    # Retrieve data rows from sqlite
    df = pd.read_sql(f"SELECT * from {TABLE}", conn)

    # Operate delete command on pandas
    delete_list = []  # [d['uuid'] for d in commands['create']]
    df = df[~df['uuid'].isin(delete_list)]

    # Operate update command on pandas
    update_list = pd.DataFrame(commands['update'])
    df.update(update_list)

    # Operate create command on pandas
    create_list = pd.DataFrame(commands['create'])
    df = pd.concat([df, create_list],
                   ignore_index=True).drop_duplicates().reset_index(drop=True)

    # Save dataframe in sqlite
    df.to_sql(name=TABLE, con=conn, if_exists='replace', index=False,
              dtype={'uuid': 'CHAR(36) PRIMARY KEY',
                     'author': 'VARCHAR(64)',
                     'message': 'VARCHAR(1024)',
                     'likes': 'unsigned INT(10)'})

    with open(time_filename, 'w') as f:
        f.write('%d' % updated_at)

    return df


cur = conn.cursor()
sqlite_create_table(cur)
sqlite_delete_all(cur)
cur.close()
df_result = main()

start = time.time()
# Option 1
cur = conn.cursor()
sqlite_print_all(cur)
cur.close()

# Option 2
# pd_to_csv(df_result)
end = time.time()

with open('time_used.txt', 'w') as f:
    f.write('%f' % (end-start))

conn.close()
