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
time_used = []

# Retrieve latest update time
with open(time_filename, 'r') as f:
    latest = f.readlines()[-1]


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
    # commands = {'create': [], 'update': [], 'delete': []}
    commands = pickle.load(a_file)
    date = datetime.datetime.utcnow()
    updated_at = calendar.timegm(date.utctimetuple())
    return (commands, updated_at)


def retrieve_commands():     # Retrieve command list from server
    while True:
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
    commands, updated_at = retrieve_commands_from_file("new_commands.pkl")

    # Retrieve data rows from sqlite
    start = time.time()
    df = pd.read_sql(f"SELECT * from {TABLE}", conn)
    end = time.time()
    time_used.append("Read:\t\t{:.5f} s".format(end-start))

    # Operate delete command on pandas
    start = time.time()
    if commands['delete'] != []:
        delete_list = commands['delete']
        df = df[~df['uuid'].isin(delete_list)]
    end = time.time()
    time_used.append("Delete:\t\t{:.5f} s".format(end-start))

    # Operate update command on pandas
    start = time.time()
    if commands['update'] != []:
        update_list = pd.DataFrame(commands['update'])
        df.set_index(['uuid'], inplace=True)
        df.update(update_list.set_index(['uuid']))
        df.reset_index(inplace=True)
    end = time.time()
    time_used.append("Update:\t\t{:.5f} s".format(end-start))

    # Operate create command on pandas
    start = time.time()
    if commands['create'] != []:
        create_list = pd.DataFrame(commands['create'])
        df = pd.concat([df, create_list],
                       ignore_index=True).drop_duplicates().reset_index(drop=True)
    end = time.time()
    time_used.append("Create:\t\t{:.5f} s".format(end-start))

    # Save dataframe in sqlite
    start = time.time()
    if list(commands.values()) != [[], [], []]:
        df.to_sql(name=TABLE, con=conn, if_exists='replace', index=False,
                  dtype={'uuid': 'CHAR(36) PRIMARY KEY',
                         'author': 'VARCHAR(64)',
                         'message': 'VARCHAR(1024)',
                         'likes': 'unsigned INT(10)'})
        end = time.time()
        _start = time.time()
        cur = conn.cursor()
        cur.execute('''PRAGMA synchronous = OFF''')
        cur.execute('''PRAGMA journal_mode = OFF''')
        cur.execute('VACUUM')
        cur.close()
        _end = time.time()
    else:
        end = time.time()
    time_used.append("Save:\t\t{:.5f} s".format(end-start))
    time_used.append("Clean-up:\t\t{:.5f} s".format(_end-_start))
    # Update latest update time
    with open(time_filename, 'a+') as f:
        f.write('%d\n' % updated_at)

    return df


cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
sqlite_create_table(cur)
cur.close()
df_result = main()


# Option 1
cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
start = time.time()
sqlite_print_all(cur)
end = time.time()
time_used.append("Sqlite-csv:\t{:.5f} s".format(end-start))
cur.close()

# Option 2
start = time.time()
pd_to_csv(df_result)
end = time.time()
time_used.append("Pandas-csv:\t{:.5f} s".format(end-start))

# Save time used log
with open('time_used.txt', 'w') as f:
    f.writelines("%s\n" % l for l in time_used)

conn.close()
