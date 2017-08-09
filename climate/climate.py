# -*- coding: utf-8 -*-
import datetime
import logging

import numpy as np
import netCDF4
import os

from climate.data_fetcher import AIR_DATA_FILE, NOAA_SERVER, fetch_data


class ClimateData(object):
    def __init__(self, data_dir):
        air_data_file = os.path.join(data_dir, AIR_DATA_FILE)

        self.air_data = loadAirData(air_data_file)

    def get_data(self, year, month, lat, lon):
        if not self.air_data:
            return ""

        # air_data lons are eastings
        lon = lon + 180

        date = datetime.datetime(year=year, month=month, day=1)

        date_search = np.where(self.air_data['dates'] == date)[0]

        if len(date_search) == 0:
            # date doesn't exist
            return ""

        date_idx = date_search[0]

        # Find the nearest latitude and longitude. subtracts lat arg from each lat in array, then gets abs val and
        # returns index of min value in array
        lat_idx = np.abs(self.air_data['lats'] - lat).argmin()
        lon_idx = np.abs(self.air_data['lons'] - lon).argmin()

        return self.air_data['air'][date_idx][lat_idx][lon_idx]


def loadAirData(file):
    """
    returns a dict of dates, lats, lons, and air. Each value is an array. The shape of the air multi-dim array is
    [date][lat][lon] which will return the mean temperature for the date, at the lat & lon
    """
    if not os.path.exists(file):
        s = input('Could not find air climate data file: `{}`. Would you like to download this dependency from '
                  '`{}`? (y/n):'.format(file, NOAA_SERVER))

        if s.lower() not in ['y', 'yes']:
            logging.info("air climate data file not found " + file)
            return

        fetch_data(file.rsplit('/', 1)[0])

    nc = netCDF4.Dataset(file)

    tname = "time"

    nctime = nc.variables[tname][:]  # get values
    t_unit = nc.variables[tname].units  # get unit  "days since 1800-01-01T00:00:00Z"
    dates = netCDF4.num2date(nctime, units=t_unit, calendar="gregorian")

    lats = nc.variables['lat'][:]
    lons = nc.variables['lon'][:]
    air = nc.variables['air'][:]  # shape is time,lat,lon

    return {
        'dates': dates,
        'lats': lats,
        'lons': lons,
        'air': air
    }
