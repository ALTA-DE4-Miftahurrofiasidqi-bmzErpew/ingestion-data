import pandas as pd

year, month, day, hour = 2017, 10, 2, 2
url = f"http://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"

# 'User-Agent' header to ensure that API requests are identified as coming from Pandas.
storage_options = {"User-Agent": "pandas"}

df = pd.DataFrame()

with pd.read_json(
    url, lines=True, storage_options=storage_options, chunksize=1000, compression="gzip"
) as reader:
    for chunk in reader:
        df = pd.concat([df, chunk], ignore_index=True)
        # print(df)

# print(df)
print(df.columns)


# Menampilkan DataFrame untuk melihat strukturnya
print(df.head())

root_df = df[["id", "type", "public", "created_at"]]
print(root_df)

# # Mengakses kolom nested, misalnya 'actor' -> 'login'
repo_df = pd.json_normalize(df["actor"])
print(repo_df)

# Mengakses kolom 'repo'
repo_df = pd.json_normalize(df["repo"])
print(repo_df)

# 'id', 'type', 'actor', 'repo', 'payload', 'public', 'created_at', 'org'
