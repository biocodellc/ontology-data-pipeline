# -*- coding: utf-8 -*-


"""proprocessor.PreProcessor: abstract class for preprocessing data before triplifying."""

import os, shutil, csv


class PreProcessor(object):
    """
    This is an abstract class that all data preprocessors need to implement.

    Attributes:
        input_dir: The directory containing the input file to process
        output_dir: The directory to write the processed data to.
        output_file_path: The file path for the processed data csv
        headers: The columns the output_file_path attribute will contain
        _out_file: The file instance that we write the processed data to.


    Abstract Methods:
        __process_data: This method is called from the run method and is where the actual data processing should be done.
            The _out_file attribute will be opened/closed for you and should be used to write you processed data.
            processed data to the __out_file file attribute. The __out_file will be opened and closed for you. See the
            headers attribute for the columns expected to be written to the __out_file attribute.

    The __input_dir is available as a private class attribute.
    """
    headers = ['record_id', 'scientific_name', 'genus', 'specific_epithet', 'year', 'day_of_year', 'latitude',
               'longitude', 'source', 'phenophase_name', 'lower_count', 'upper_count', 'lower_percent', 'upper_percent']

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_file_path = os.path.join(output_dir, 'data.csv')
        self._out_file = None

    def run(self):
        self.__clean()

        self._out_file = open(self.output_file_path, 'w')
        writer = csv.writer(self._out_file)
        writer.writerow(self.headers)

        self._process_data()

        self._out_file.close()

    def _process_data(self):
        raise NotImplementedError("__process_data has not been implemented")

    def __clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)
