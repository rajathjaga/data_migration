import requests, zipfile, io

def download_and_unzip_data(url, download_path, extract_to):
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(extract_to)
    print("Data downloaded and unzipped successfully.")