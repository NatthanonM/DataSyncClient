import requests
import sqlite3
import calendar
import datetime
from urllib.parse import urljoin
import sys
import pickle

conn = sqlite3.connect('my.db')
cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
cur.execute("PRAGMA auto_vacuum = FULL")
TABLE = 'data_record'
my_arg = sys.argv
base_url = my_arg[1]
# endpoint = ".../api/messages/"
endpoint = {'create': f"{base_url}/api/messages/create/",
            'update-delete': f"{base_url}/api/messages/update-delete/"}

time_filename = "time.txt"
time_used = []


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
    data = {'uuid': data[0], 'author': data[1],
            'message': data[2], 'likes': data[3]}
    statement = f"INSERT INTO {TABLE} VALUES (:uuid, :author, :message, :likes)"
    args = dict(data)
    cur.execute(statement, args)


def sqlite_multiple_insert(create_list):    # Insert multiple row
    statement = f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?)"
    create_list = [tuple(d) for d in create_list]
    cur.executemany(statement, create_list)


def sqlite_update(data):    # Update a row of data
    pkey = data['u']
    del data['u']
    key_dict = {'u': 'uuid', 'a': 'author', 'm': 'message', 'l': 'likes'}
    set_keys = ", ".join(
        [f"{key_dict[k]}=:{k}" for k in data.keys()])
    statement = f"UPDATE {TABLE} SET {set_keys} WHERE {'uuid'} = :u"
    args = dict(data)
    args['u'] = pkey
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


# Retrieve command list from server
def retrieve_commands_create(latest, latest_uuid):
    while True:
        # Retrieve commands
        latest_info = latest + '/' + latest_uuid
        r = requests.get(urljoin(endpoint['create'], latest_info))
        if r.status_code == 200:
            commands = r.json()
            break
    return commands


def retrieve_commands_update_delete(latest):
    while True:
        # Retrieve commands
        r = requests.get(urljoin(endpoint['update-delete'], latest))
        if r.status_code == 200:
            commands = r.json()
            break
    return commands


def reset_data_sync():
    sqlite_delete_all()
    with open(time_filename, 'w') as f:
        f.write('1640970001\n')


def main():
    # Retrieve latest update time
    with open(time_filename, 'r') as f:
        latest = f.readlines()[-1].strip()
    # Declare present time
    date = datetime.datetime.utcnow()
    updated_at = calendar.timegm(date.utctimetuple())

    # CREATE UPDATE-DELETE SECTION
    commands = retrieve_commands_update_delete(latest)

    # Operate delete command
    # start = time.time()
    for delete_command in commands['d']:
        sqlite_delete(delete_command)
    # end = time.time()
    # time_used.append("Delete:\t\t{:.5f} s".format(end-start))

    del commands['d']

    # Operate update command
    # start = time.time()
    for update_command in commands['u']:
        sqlite_update(update_command)
    # end = time.time()
    # time_used.append("Update:\t\t{:.5f} s".format(end-start))

    del commands

    # CREATE SECTION
    latest_uuid = "0"
    while True:
        commands = retrieve_commands_create(latest, latest_uuid)
        commands = commands['c']
        if len(commands) == 0:
            break
        # Operate create command
        # start = time.time()
        sqlite_multiple_insert(commands)
        # end = time.time()
        # time_used.append("Create:\t\t{:.5f} s".format(end-start))
        latest_uuid = commands[-1][0]
        if len(commands) < 100000:
            break

    del commands

    # Commit data changes
    # start = time.time()
    conn.commit()
    # end = time.time()
    # time_used.append("Commit:\t\t{:.5f} s".format(end-start))

    # Update latest update time
    with open(time_filename, 'a+') as f:
        f.write('%d\n' % updated_at)


sqlite_create_table()
# reset_data_sync()
# retrieve_commands()
main()

# start = time.time()
sqlite_print_all()
# end = time.time()
# time_used.append("Sqlite-csv:\t{:.5f} s".format(end-start))

# Save time used log
# with open('time_used.txt', 'w') as f:
# f.writelines("%s\n" % l for l in time_used)

conn.close()
