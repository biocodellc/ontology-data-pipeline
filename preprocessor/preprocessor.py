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
        headers: Read in the headers from the config/headers.csv file


    Abstract Methods:
        __process_data: This method is called from the run method and is where the actual data processing should be done.

    The __input_dir is available as a private class attribute.
    """

    def __init__(self, input_dir, output_dir, headers_file):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_file = os.path.join(output_dir, 'data.csv')
        self.headers = headers_file

    def run(self):
        self.__clean()

        self._write_headers()

        self._process_data()

    def _write_headers(self):
        #print(self.headers_file)
        with open(self.output_file, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(self.headers)

    def _process_data(self):
        raise NotImplementedError("__process_data has not been implemented")

    def __clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

