# -*- coding: utf-8 -*-

import argparse
import ftplib
import os

"""script to harvest climate data from noaa."""

NOAA_SERVER = "ftp.cdc.noaa.gov"
AIR_DATA_DIR = "/pub/Datasets/ncep.reanalysis.derived/surface/"

AIR_DATA_FILE = "air.mon.mean.nc"


def fetch_data(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    download_file(os.path.join(output_dir, AIR_DATA_FILE))


def download_file(output_path):
    ftp = ftplib.FTP(NOAA_SERVER)
    ftp.login("", "")
    ftp.cwd(AIR_DATA_DIR)

    ftp.retrbinary("RETR " + AIR_DATA_FILE, open(output_path, 'wb').write)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NEON Data Fetcher')
    default_dir = os.path.join(os.path.dirname(__file__), "../data/climate_data")
    parser.add_argument('output_dir', help='the directory to place the data', default=default_dir, nargs="?")

    args = parser.parse_args()
    output_dir = args.output_dir.strip()

    fetch_data(output_dir)
