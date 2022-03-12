import requests
import sqlite3
import calendar
import datetime
from urllib.parse import urljoin
import time
import pickle

conn = sqlite3.connect('my.db')
cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
TABLE = 'data_record'
# endpoint = ".../api/messages/"
endpoint = ""
time_filename = "time.txt"
time_used = []

# Retrieve latest update time
with open(time_filename, 'r') as f:
    latest = f.readlines()[-1]


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


def sqlite_insert(data):    # Insert a row of data
    statement = f"INSERT INTO {TABLE} VALUES (:uuid, :author, :message, :likes)"
    args = dict(data)
    cur.execute(statement, args)


def sqlite_multiple_insert(create_list):    # Insert multiple row
    create_tuple = [
        (d['uuid'], d['author'], d['message'], d['likes']) for d in create_list]
    statement = f"INSERT INTO {TABLE} VALUES (?,?,?,?)"
    cur.executemany(statement, create_tuple)


def sqlite_update(data):    # Update a row of data
    pkey = data['uuid']
    del data['uuid']
    set_keys = ", ".join([f"{k}=:{k}" for k in data.keys()])
    statement = f"UPDATE {TABLE} SET {set_keys} WHERE {'uuid'} = :uuid"
    args = dict(data)
    args['uuid'] = pkey
    cur.execute(statement, args)


def sqlite_delete(pkey):    # Delete a row of data
    statement = f"DELETE FROM {TABLE} WHERE {'uuid'} = :uuid"
    cur.execute(statement, {'uuid': pkey})


def sqlite_print_all():     # Query data and print it in csv format
    for row in cur.execute("SELECT * FROM data_record"):
        print(*row, sep=",")


def retrieve_commands_from_file(filename):
    a_file = open(filename, "rb")
    # commands = {'create': [], 'update': [], 'delete': []}
    commands = pickle.load(a_file)
    date = datetime.datetime.utcnow()
    updated_at = calendar.timegm(date.utctimetuple())
    return (commands, updated_at)


def retrieve_commands():    # Retrieve command list from server
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


def main():
    commands, updated_at = retrieve_commands_from_file("new_commands.pkl")

    start = time.time()
    end = time.time()
    time_used.append("Read:\t\t{:.5f} s".format(end-start))

    # Operate delete command
    start = time.time()
    for delete_command in commands['delete']:
        sqlite_delete(delete_command)
    end = time.time()
    time_used.append("Delete:\t\t{:.5f} s".format(end-start))

    # Operate update command
    start = time.time()
    for update_command in commands['update']:
        sqlite_update(update_command)
    end = time.time()
    time_used.append("Update:\t\t{:.5f} s".format(end-start))

    # Operate create command
    start = time.time()
    sqlite_multiple_insert(commands['create'])
    end = time.time()
    time_used.append("Create:\t\t{:.5f} s".format(end-start))

    start = time.time()
    conn.commit()
    end = time.time()
    time_used.append("Commit:\t\t{:.5f} s".format(end-start))

    start = time.time()
    cur.execute('VACUUM')
    end = time.time()
    time_used.append("Clean-up:\t\t{:.5f} s".format(end-start))

    # Update latest update time
    with open(time_filename, 'a+') as f:
        f.write('%d\n' % updated_at)


sqlite_create_table()
# sqlite_delete_all()
# retrieve_commands()
main()

start = time.time()
sqlite_print_all()
end = time.time()
time_used.append("Sqlite-csv:\t{:.5f} s".format(end-start))

# Save time used log
with open('time_used.txt', 'w') as f:
    f.writelines("%s\n" % l for l in time_used)

conn.close()
