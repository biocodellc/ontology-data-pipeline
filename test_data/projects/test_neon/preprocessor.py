# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing neon data"""

import logging
import multiprocessing
import pandas as pd

from zipfile import ZipFile
from preprocessor import AbstractPreProcessor

from projects.neon.helpers import walk_files, INTENSITY_VALUE_FRAME

COLUMNS_MAP = {
    'uid': 'record_id',
    'dayOfYear': 'day_of_year',
    'scientificName': 'scientific_name',
    'phenophaseName': 'phenophase_name',
    'decimalLatitude': 'latitude',
    'decimalLongitude': 'longitude'
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        num_processes = multiprocessing.cpu_count()
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.map(self._process_zip, [f for f in walk_files(self.input_dir)])

    def _process_zip(self, file):
        statusintensity_file = None
        per_individual_file = None

        logging.debug("\tprocessing {}".format(file))
        with ZipFile(file) as zip_file:
            for filename in zip_file.namelist():

                if '.phe_statusintensity.' in filename:
                    if statusintensity_file:
                        raise RuntimeError(
                            'multiple phe_statusintensity csv files found in zip_file {}'.format(zip_file.filename))
                    statusintensity_file = zip_file.open(filename)

                elif '.phe_perindividual.' in filename:
                    if per_individual_file:
                        raise RuntimeError(
                            'multiple phe_perindividual csv files found in zip_file {}'.format(zip_file.filename))
                    per_individual_file = zip_file.open(filename)

        if not statusintensity_file or not per_individual_file:
            raise RuntimeError('could not find needed files in zip_file {}'.format(zip_file.filename))

        individuals = pd.read_csv(per_individual_file, header=0, skipinitialspace=True,
                                  usecols=['decimalLatitude', 'decimalLongitude', 'namedLocation', 'individualID',
                                           'scientificName', 'addDate'])

        # take the latest entry of an individualID as the source of truth
        individuals = individuals.sort_values('addDate', ascending=False).drop_duplicates('individualID')

        data = pd.read_csv(statusintensity_file, header=0, skipinitialspace=True,
                           usecols=['uid', 'date', 'dayOfYear', 'individualID', 'phenophaseName', 'phenophaseStatus',
                                    'phenophaseIntensity', 'namedLocation'], parse_dates=['date'])

        data = data.merge(individuals, left_on=['individualID', 'namedLocation'],
                          right_on=['individualID', 'namedLocation'], how='left')

        self._transform_data(data).to_csv(self.output_file, columns=self.headers, mode='a', header=False, index=False)

        statusintensity_file.close()
        per_individual_file.close()

    @staticmethod
    def _transform_data(data):
        data['source'] = 'NEON'
        data['genus'] = data.apply(lambda row: row.scientificName.split()[0] if pd.notnull(row.scientificName) else "",
                                   axis=1)
        data['specific_epithet'] = data.apply(
            lambda row: row.scientificName.split()[1] if pd.notnull(row.scientificName) else "", axis=1)
        data['year'] = data.apply(lambda row: row['date'].year, axis=1)
        # original data had dayOfYear, but latest download doesn't. Try and fill from date
        data['dayOfYear'] = data.apply(
            lambda row: row['date'].timetuple().tm_yday if pd.isnull(row['dayOfYear']) and pd.notnull(row['date']) else
            row['dayOfYear'], axis=1)

        df = data.merge(INTENSITY_VALUE_FRAME, left_on='phenophaseIntensity', right_on='value', how='left')

        # check that we have a 'value' value if we have a phenophaseIntensity value
        if not df.loc[df.phenophaseIntensity.notnull() & df.value.isnull()].empty:
            raise RuntimeError(
                'found row with a phenophaseIntensity, but is missing the appropriate counts. may need to '
                'regenerate the intensity_values.csv file. Run the Neon helpers.py script with the --intensity flag to '
                'append "values" that do not currently exist in the intensity_values.csv file. You will '
                'need to manually insert the correct counts in the intensity_values.csv file.')

        # if phenophaseStatus is 'yes' and no phenophaseIntensity, set lower_count = 1
        df.loc[df.phenophaseIntensity.isnull() & df.phenophaseStatus.str.match('yes', case=False), 'lower_count'] = 1
        # if phenophaseStatus is 'yes' and no phenophaseIntensity, set lower_count = 1
        df.loc[df.phenophaseIntensity.isnull() & df.phenophaseStatus.str.match('no', case=False), 'upper_count'] = 0

        # JBD removing these two lines as they lead to inconsistent ontology errors
        #df["lower_count"] = df["lower_count"].fillna(0.0).astype(int)
        #df["upper_count"] = df["upper_count"].fillna(0.0).astype(int)
        #df.fillna('', inplace=True)  # replace all null values

        return df.rename(columns=COLUMNS_MAP)
