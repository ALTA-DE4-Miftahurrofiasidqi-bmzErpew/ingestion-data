import pandas as pd

df  = pd.read_json('../dataset/2017-10-02-1.json', lines=True, orient='columns', nrows=5)

# Mengakses kolom 'id', 'type', 'public', 'created_at'
root_df = df[['id', 'type', 'public', 'created_at']]
print(root_df)

# # Mengakses kolom nested, misalnya 'actor' -> 'login'
repo_df = pd.json_normalize(df['actor'])
print(repo_df)

# Mengakses kolom 'repo'
repo_df = pd.json_normalize(df['repo'])
print(repo_df)


# {
#     "id":"6660512193",
#     "type":"WatchEvent",
#     "actor":{
#         "id":32378521,
#         "login":"wqingt023",
#         "display_login":"wqingt023",
#         "gravatar_id":"",
#         "url":"https://api.github.com/users/wqingt023",
#         "avatar_url":"https://avatars.githubusercontent.com/u/32378521?"
#     },
#     "repo":{
#         "id":105219730,
#         "name":"wqingt023/ud120-projects",
#         "url":"https://api.github.com/repos/wqingt023/ud120-projects"
#     },
#     "payload":{
#         "action":"started"
#     },
#     "public":true,
#     "created_at":"2017-10-02T01:00:00Z"
# }
