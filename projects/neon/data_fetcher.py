import argparse
import os
import requests
import json
import shutil

"""script to harvest data using the neon api. All file in output_dir will be erased before downloading"""

AVAILABLE_DATA_ENDPOINT = "http://data.neonscience.org/api/v0/products/DP1.10055.001"


def fetch_data(output_dir):
    clean_dir(output_dir)
    site_metadata = fetch_metadata(AVAILABLE_DATA_ENDPOINT)

    for site in site_metadata['data']['siteCodes']:

        for url in site['availableDataUrls']:
            metadata = fetch_metadata(url)

            files_to_download = []
            for f in metadata['data']['files']:
                if f['name'].endswith('.zip'):
                    files_to_download.append(f)

            if len(files_to_download) != 1:
                print("expected to download only 1 zip file, but have {}: {}".format(len(files_to_download),
                                                                                     files_to_download))

            for f in files_to_download:
                download_file(f['url'], os.path.join(output_dir, f['name']))


def fetch_metadata(url):
    r = requests.get(url)

    if r.status_code != 200:
        raise Exception("Invalid response returned from {}: {}".format(url, r.content))

    return json.loads(r.content)


def clean_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)


def download_file(url, output_path):
    r = requests.get(url, stream=True)

    if r.status_code is not 200:
        print('error downloading {}: status_code: {}\n{}'.format(url, r.status_code, r.content))

    with open(output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)

    r.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NEON Data Fetcher')
    parser.add_argument('output_dir', help='the directory to place the data')

    args = parser.parse_args()
    output_dir = args.output_dir.strip()

    fetch_data(output_dir)
