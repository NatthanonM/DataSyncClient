import requests
import sqlite3
import calendar
import datetime
from urllib.parse import urljoin

conn = sqlite3.connect('my.db')
cur = conn.cursor()
cur.execute('''PRAGMA synchronous = OFF''')
cur.execute('''PRAGMA journal_mode = OFF''')
TABLE = 'data_record'
# endpoint = ".../api/messages/"
endpoint = ""


def sqlite_create_table():    # Create table
    statement = '''CREATE TABLE `data_record` (
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


def retrieve_commands():
    while True:
        with open('time.txt') as f:
            latest = f.readline()
        # Retrieve commands1646957437
        date = datetime.datetime.utcnow()
        updated_at = calendar.timegm(date.utctimetuple())
        r = requests.get(urljoin(endpoint, latest))
        if r.status_code == 200:
            commands = r.json()
            break
    return (commands, updated_at)


def main():
    commands, updated_at = retrieve_commands()

    # Operate delete command
    for delete_command in commands['delete']:
        sqlite_delete(delete_command)
    else:
        conn.commit()
        cur.execute('VACUUM')

    # Operate create command
    sqlite_multiple_insert(commands['create'])

    # Operate update command
    for update_command in commands['update']:
        sqlite_update(update_command)

    conn.commit()
    with open('time.txt', 'w') as f:
        f.write('%d' % updated_at)


# sqlite_create_table()
# sqlite_delete_all()
# retrieve_commands()
main()
sqlite_print_all()

conn.close()
