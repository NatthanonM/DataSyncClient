import pickle
import uuid
a_file = open("commands.pkl", "rb")
commands = pickle.load(a_file)
a_file.close()

present_uuid = [d['uuid'] for d in commands['create']]
delete_uuid = present_uuid[:33333]
update_uuid = present_uuid[33333:66666]
create_uuid = []
for i in range(33333):
    while True:
        _uuid = str(uuid.uuid4())
        if _uuid not in present_uuid:
            create_uuid.append(_uuid)
            break

commands_out = {'create': [{'uuid': _uuid, 'author': 'james', 'message': "I knew you'd haunt all of my what ifs", 'likes': 2222} for _uuid in create_uuid[:11111]] +
                [{'uuid': _uuid, 'author': 'inez', 'message': 'When you are young, they assume you know nothing', 'likes': 19893} for _uuid in create_uuid[11111:22222]] +
                [{'uuid': _uuid, 'author': 'drew', 'message': 'Vintage tee, brand new phone', 'likes': 1312}
                    for _uuid in create_uuid[22222:]],
                'update': [{'uuid': _uuid, 'author': 'betty', 'message': 'I bet you think about me', 'likes': 1989} for _uuid in update_uuid[:11111]] +
                [{'uuid': _uuid, 'message': 'Forever winter if you go', 'likes': 1313} for _uuid in update_uuid[11111:22222]] +
                [{'uuid': _uuid, 'author': 'taylor'}
                    for _uuid in update_uuid[22222:]],
                'delete': delete_uuid}

a_file = open("new_commands.pkl", "wb")
pickle.dump(commands_out, a_file)
a_file.close()
