# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing asu data"""

import uuid, os
import logging
import pandas as pd
from preprocessor import AbstractPreProcessor

ASU_DATA_DIR = 'ASU_Phenology_DWCA'

COLUMNS_MAP = {
    'scientificName': 'scientific_name',
    'specificEpithet': 'specific_epithet',
    'startDayOfYear': 'day_of_year',
    'measurementValue': 'phenophase_name',
    'decimalLatitude': 'latitude',
    'decimalLongitude': 'longitude'
}

FILES = {
    'data': 'data.csv',
    'occurrences': os.path.join(ASU_DATA_DIR, 'occurrences.csv'),
    'identifications': os.path.join(ASU_DATA_DIR, 'identifications.csv'),
    'measurements': os.path.join(ASU_DATA_DIR, 'measurementOrFact.csv')
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        self.occurrences = pd.read_csv(self.input_dir + FILES['occurrences'], header=0, skipinitialspace=True,
                                       usecols=['occurrenceID', 'scientificName', 'genus', 'specificEpithet', 'year',
                                                'startDayOfYear', 'decimalLatitude', 'decimalLongitude', 'id'])

        data = pd.read_csv(self.input_dir + FILES['measurements'], header=0, skipinitialspace=True, chunksize=100000,
                           usecols=['coreid', 'measurementValue'])

        for chunk in data:
            logging.debug("\tpreprocessing {} records".format(len(chunk)))
            self.__transform_data(chunk).to_csv(self.output_file, columns=self.headers, mode='a', header=False,
                                                index=False)

    def __transform_data(self, base_data):
        data = base_data \
            .merge(self.occurrences, left_on='coreid', right_on='id', how='left')

        data['record_id'] = data.apply(lambda x: uuid.uuid4(), axis=1)

        data.fillna("", inplace=True)  # replace all null values

        data['source'] = 'ASU'
        data['lower_count'] = 1

        return data.rename(columns=COLUMNS_MAP)
