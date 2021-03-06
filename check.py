import pandas as pd
import requests
import sys

my_arg = sys.argv
base_url = my_arg[1]
endpoint = f"{base_url}/api/messages/all-no-delete"

print("Getting data from output csv file...")

my_df = pd.read_csv("output.csv", header=None, names=[
                    'uuid', 'author', 'message', 'likes'])
# print(my_df)

print("Getting data from server database...")
r = requests.get(endpoint)
if r.status_code == 200:
    data = r.json()
else:
    print(r.status_code, r.text)
    exit()
server_df = pd.DataFrame(
    data=data['d'], columns=['uuid', 'author', 'message', 'likes'])
# print(server_df)

print("Comparing the difference...")
df_diff = pd.merge(my_df, server_df, how='outer', indicator='Exist')
df_diff = df_diff.loc[df_diff['Exist'] != 'both']
if df_diff.shape[0] == 0:
    print("There is no difference.")
else:
    print("Here is the differnce:")
    print(len(df_diff['uuid'].unique()))
    print("left only:", df_diff[df_diff['Exist'] == 'left_only'])
    print("right only:", df_diff[df_diff['Exist'] == 'right_only'])
