import requests
from tqdm import tqdm

# Fungsi untuk mengunduh file dengan progress bar
def download_file(url, dest):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte
    t = tqdm(total=total_size, unit="iB", unit_scale=True)
    with open(dest, "wb") as file:
        for data in response.iter_content(block_size):
            t.update(len(data))
            file.write(data)
    t.close()
    if total_size != 0 and t.n != total_size:
        print("Error: Something went wrong during the download")
        return False
    return True



url = "https://github.com/Immersive-DataEngineer-Resource/ingestion-data/dataset/2017-10-02-1.json"

download_file(url, "2017-10-02-1.json")