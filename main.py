import uuid
import sqlite3
import json
conn = sqlite3.connect('my.db')
cur = conn.cursor()
TABLE = 'data_record'
# Create table
# statement = '''CREATE TABLE `data_record` (
# 	`uuid` CHAR(36),
# 	`author` VARCHAR(64),
# 	`message` VARCHAR(1024),
# 	`likes` unsigned INT(10),
# 	PRIMARY KEY (`uuid`)
# );'''
# cur.execute(statement, args)
# conn.commit()


def sqlite_insert(data):    # Insert a row of data
    statement = f"INSERT INTO {TABLE} VALUES (:uuid, :author, :message, :likes)"
    args = dict(data)
    cur.execute(statement, args)


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
        print(*row, sep=", ")


# Retrieve commands
f = open('commands.json', 'rb')
commands = json.load(f)
f.close()

# Operate delete command
for delete_command in commands['DELETE']:
    sqlite_delete(delete_command)
else:
    conn.commit()

# Operate create command
for create_command in commands['CREATE']:
    sqlite_insert(create_command)
else:
    conn.commit

# Operate update command
for update_command in commands['UPDATE']:
    sqlite_update(update_command)
else:
    conn.commit

sqlite_print_all()

conn.close()
