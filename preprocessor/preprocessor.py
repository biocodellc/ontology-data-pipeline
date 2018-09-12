# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor: abstract class for preprocessing data before triplifying."""
import datetime
import logging
import os, shutil, csv

import multiprocessing

import pandas as pd

class AbstractPreProcessor(object):
    """
    This is an abstract class that all data preprocessors need to implement.

    Attributes:
        input_dir: The directory containing the input file to process
        output_dir: The directory to write the processed data to.
        output_file: The file path for the processed data csv
        headers: The columns the output_file_path attribute will contain


    Abstract Methods:
        __process_data: This method is called from the run method and is where the actual data processing should be done.
            See the headers attribute for the columns expected to be written to the output_file attribute.

    The __input_dir is available as a private class attribute.
    """
    headers = ['record_id', 'scientific_name', 'genus', 'specific_epithet', 'year', 'day_of_year', 'latitude',
               'longitude', 'source', 'sub_source', 'phenophase_name', 'lower_count_partplant', 'upper_count_partplant', 'lower_count_wholeplant', 'upper_count_wholeplant', 'lower_percent',
               'upper_percent']

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_file = os.path.join(output_dir, 'data.csv')

    def run(self):
        self.__clean()

        self._write_headers()

        self._process_data()

    def _write_headers(self):
        with open(self.output_file, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(self.headers)

    def _process_data(self):
        raise NotImplementedError("__process_data has not been implemented")

    def __clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

