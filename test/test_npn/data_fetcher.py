# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import xml.etree.ElementTree

import requests
import shutil

"""script to harvest data using the npn api. All files in output_dir will be erased before downloading"""

DATA_ENDPOINT = "http://www.usanpn.org/npn_portal/observations/getObservations.xml?request_src=ppo-data-pipeline_rest&additional_field=dataset_id"

HEADERS = ['genus', 'species', 'observation_id', 'observation_date', 'dataset_id', 'day_of_year', 'intensity_value',
           'latitude', 'longitude', 'phenophase_description', 'phenophase_status', ]


def fetch_data(output_dir):
    clean_dir(output_dir)

    r = requests.get(DATA_ENDPOINT, stream=True)

    if r.status_code is not 200:
        print('error downloading data: status_code: {}\n{}'.format(r.status_code, r.content))
        exit()

    p = re.compile(r'(<observation .* />)')

    with open(os.path.join(output_dir, 'npn_observations_data.csv'), 'w') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction='ignore')

        writer.writeheader()

        # for chunk in r.iter_content(chunk_size=1024):
        for chunk in r.iter_lines():
            if chunk:  # filter out keep-alive new chunks
                observations = p.findall(str(chunk))  # parse the xml for observations

                # convert xml to csv
                for o in observations:
                    e = xml.etree.ElementTree.fromstring(o)

                    writer.writerow(e.attrib)


def clean_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NPN Data Fetcher')
    parser.add_argument('output_dir', help='the directory to place the data')

    args = parser.parse_args()
    output_dir = args.output_dir.strip()

    fetch_data(output_dir)
