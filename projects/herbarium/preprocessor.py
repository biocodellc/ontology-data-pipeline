# -*- coding: utf-8 -*-

"""proprocessor.AbstractPreProcessor implementation for preprocessing pep725 data"""

import re, uuid
import os
import multiprocessing
import pandas as pd
import math
from preprocessor import AbstractPreProcessor

PHENOPHASE_DESCRIPTIONS_FILE = os.path.join(os.path.dirname(__file__), 'phenophase_descriptions.csv')
FILE_PREFIX = "herbarium_"
DATA_FILE = os.path.join(FILE_PREFIX+'_data.csv')

class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        self.descriptions = pd.read_csv(PHENOPHASE_DESCRIPTIONS_FILE, header=0, skipinitialspace=True, dtype='object')

        num_processes = multiprocessing.cpu_count()
        chunk_size = 100000
        data = pd.read_csv(os.path.join(self.input_dir, "herbarium_data.csv"), header=0, chunksize=chunk_size*num_processes)

        for chunk in data:
            chunks = [chunk.ix[chunk.index[i:i + chunk_size]] for i in
                      range(0, chunk.shape[0], chunk_size)]

            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.map(self._transform_chunk, chunks)

    def _transform_chunk(self, chunk):
        self._transform_data(chunk).to_csv(self.output_file, columns=self.headers, mode='a', header=False, index=False)

    def _transform_data(self, data):

        data.fillna("", inplace=True)  # replace all null values

        # Create a unique record ID for each observation
        data['record_id'] = data.apply(lambda x: uuid.uuid4(), axis=1)

        # Capitalize genus
        data['genus'] = data['genus'].str.capitalize()

        # Create ScientificName
        data['scientific_name'] = data['genus'] + ' ' + data['specific_epithet']

        # Set default lower and upper counts
        data = data.apply(lambda row: self._set_defaults(row), axis=1)

        return data

    def _set_defaults(self, row):
        try:
            row['lower_count'] = self.descriptions[self.descriptions['field'] == row['phenophase_name']]['lower_count'].values[0]
            row['upper_count'] = self.descriptions[self.descriptions['field'] == row['phenophase_name']]['upper_count'].values[0]

            row['lower_count_partplant'] = self.descriptions[self.descriptions['field'] == row['phenophase_name']]['lower_count_partplant'].values[0]
            row['upper_count_partplant'] = self.descriptions[self.descriptions['field'] == row['phenophase_name']]['upper_count_partplant'].values[0]
        except IndexError:
            # thrown if missing phenophase_description in phenophase_descriptions.csv file
            pass

        return row
