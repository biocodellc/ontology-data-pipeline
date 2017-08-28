# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor: abstract class for preprocessing data before triplifying."""
import datetime
import logging
import os, shutil, csv

import multiprocessing

import pandas as pd

from climate import climate

CLIMATE_DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/climate_data/")


class AbstractPreProcessor(object):
    """
    This is an abstract class that all data preprocessors need to implement.

    Attributes:
        input_dir: The directory containing the input file to process
        output_dir: The directory to write the processed data to.
        output_file: The file path for the processed data csv
        climate_data: climate.ClimateData object
        headers: The columns the output_file_path attribute will contain


    Abstract Methods:
        __process_data: This method is called from the run method and is where the actual data processing should be done.
            See the headers attribute for the columns expected to be written to the output_file attribute.

    The __input_dir is available as a private class attribute.
    """
    headers = ['record_id', 'scientific_name', 'genus', 'specific_epithet', 'year', 'day_of_year', 'latitude',
               'longitude', 'source', 'sub_source', 'phenophase_name', 'lower_count', 'upper_count', 'lower_percent',
               'upper_percent', 'adjusted_ncep_reanalysis_monthly_mean_temp']

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_file = os.path.join(output_dir, 'data.csv')
        self.climate_data = climate.ClimateData(CLIMATE_DATA_DIR)

    def run(self):
        self.__clean()

        self._write_headers()

        self._process_data()
        self._add_climate_data()

    def _write_headers(self):
        with open(self.output_file, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(self.headers)

    def _process_data(self):
        raise NotImplementedError("__process_data has not been implemented")

    def _add_climate_data(self):
        logging.debug("adding climate data")

        data_file = os.path.join(self.output_dir, 'tmp_data.csv')
        shutil.move(self.output_file, data_file)

        self._write_headers()

        num_processes = multiprocessing.cpu_count()
        chunk_size = 100000

        data = pd.read_csv(data_file, header=0, skipinitialspace=True, chunksize=chunk_size * num_processes)

        for data_frame in data:
            chunks = [data_frame.ix[data_frame.index[i:i + chunk_size]] for i in
                      range(0, data_frame.shape[0], chunk_size)]

            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.map(self._add_climate_data_for_chunk, chunks)

        os.remove(data_file)

    def _add_climate_data_for_chunk(self, chunk):
        chunk['adjusted_ncep_reanalysis_monthly_mean_temp'] = chunk.apply(self._get_air_data, axis=1)

        chunk.to_csv(self.output_file, mode='a', header=False, index=False)

    def __clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

    def _get_air_data(self, row):
        if not hasattr(row, 'year') or not hasattr(row, 'latitude') or not hasattr(row, 'longitude') or \
                not hasattr(row, 'day_of_year'):
            return ""

        year = row['year']
        lat = row['latitude']
        lon = row['longitude']

        try:
            year = int(year)
            date = datetime.datetime.strptime("{} {}".format(year, int(row['day_of_year'])), '%Y %j')
        except ValueError:
            return ""

        # if an observation is made on July 1st, i don't think we want to use the July monthly mean.
        # Probably better to use the preceding month.
        # 1-14th of month use prior monthly mean
        # 15-31st use current monthly mean
        if date.day > 14:
            month = date.month
        else:
            month = date.month - 1
            if month == 0:
                year = year - 1
                month = 12
            elif month == 13:
                year = year + 1
                month = 1

        return self.climate_data.get_data(year, month, lat, lon)
