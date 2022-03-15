import uuid
import json
a_file = open("commands.json", "rb")
commands = json.load(a_file)
a_file.close()

present_uuid = [d[0] for d in commands['c']]
create_uuid = [str(uuid.uuid4()) for i in range(3333)]
# for i in range(33333):
#     while True:
#         _uuid = str(uuid.uuid4())
#         if _uuid not in present_uuid:
#             create_uuid.append(_uuid)
#             break

update_uuid = create_uuid[:1111] + present_uuid[2222:4444]
delete_uuid = present_uuid[:1111] + \
    present_uuid[2222:4444] + create_uuid[:1111] + create_uuid[1111:2222]

commands_out = {'c': [[_uuid, 'james', "I knew you'd haunt all of my what ifs",  2222] for _uuid in create_uuid[:1111]] +
                [[_uuid, 'inez', 'When you are young they assume you know nothing', 19893] for _uuid in create_uuid[1111:2222]] +
                [[_uuid, 'drew',  'Vintage tee brand new phone', 1312]
                    for _uuid in create_uuid[2222:]],
                'u': [{'u': _uuid, 'a': 'betty', 'm': 'I bet you think about me', 'l': 1989} for _uuid in update_uuid[:1111]] +
                [{'u': _uuid, 'm': 'Forever winter if you go', 'l': 1313} for _uuid in update_uuid[1111:2222]] +
                [{'u': _uuid, 'a': 'taylor'}
                    for _uuid in update_uuid[2222:]],
                'd': delete_uuid}

a_file = open("new_commands.json", "w")
json.dump(commands_out, a_file)
a_file.close()
