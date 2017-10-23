# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing npn data"""

import os, csv
import pandas as pd
from preprocessor import AbstractPreProcessor

PHENOPHASE_DESCRIPTIONS_FILE = os.path.join(os.path.dirname(__file__), 'phenophase_descriptions.csv')
DATASET_METADATA_FILE = os.path.join(os.path.dirname(__file__), 'ancillary_dataset_data.csv')
COLUMNS_MAP = {
    'observation_id': 'record_id',
    'species': 'specific_epithet',
    'phenophase_description': 'phenophase_name',
    'Dataset_Name': 'sub_source'
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        self.descriptions = pd.read_csv(PHENOPHASE_DESCRIPTIONS_FILE, header=0, skipinitialspace=True)
        self.dataset_metadata = pd.read_csv(DATASET_METADATA_FILE, header=0, skipinitialspace=True,
                                            usecols=['Dataset_ID', 'Dataset_Name'])

        chunk_size = 100000
        df = pd.read_csv(os.path.join(self.input_dir, "test_data.csv"), header=0, chunksize=chunk_size)

        for chunk in df:
            print("\tprocessing next {} records".format(len(chunk)))
            self._transform_data(chunk).to_csv(self.output_file, columns=self.headers, mode='a',
                                               header=False, index=False)

    def _transform_data(self, df):
        # Add an index name
        # df.index.name = 'record_id'

        # Translate values from intensity_values.csv file

        # translate values
        cols = ['value', 'lower_count', 'upper_count', 'lower_percent', 'upper_percent']
        df = self._translate(os.path.join(os.path.dirname(__file__), 'intensity_values.csv'), cols, 'value', df,
                             'intensity_value')

        # set upper/lower counts for cases of no intensity value
        df = df.apply(lambda row: self._set_defaults(row), axis=1)

        df['source'] = 'NPN'
        df = df.merge(self.dataset_metadata, left_on='dataset_id', right_on='Dataset_ID', how='left')

        # Normalize Date to just Year. we don't need to store actual date because we use only Year + DayOfYear
        df['year'] = pd.DatetimeIndex(df['observation_date']).year

        # Create ScientificName
        df['scientific_name'] = df['genus'] + ' ' + df['species']

        # drop duplicate ObservationIDs
        df.drop_duplicates('observation_id', inplace=True)

        return df.rename(columns=COLUMNS_MAP)

    def _set_defaults(self, row):
        if row.intensity_value != '-9999':
            return row

        try:
            print (row.phenophase_status)
            if row.phenophase_status == 0:
                row['lower_percent'] = self.descriptions[self.descriptions['field'] == row['phenophase_description']][
                    'lower_percent_absent'].values[0]
                row['upper_percent'] = self.descriptions[self.descriptions['field'] == row['phenophase_description']][
                    'upper_percent_absent'].values[0]
            else:
                row['lower_percent'] = self.descriptions[self.descriptions['field'] == row['phenophase_description']][
                    'lower_percent_present'].values[0]
                row['upper_percent'] = self.descriptions[self.descriptions['field'] == row['phenophase_description']][
                    'upper_percent_present'].values[0]
        except IndexError:
            # thrown if missing phenophase_description in phenophase_descriptions.csv file
            pass

        return row

    @staticmethod
    def _translate(filename, cols, index_name, data_frame, lookup_column):
        """
         Function to read in a CSV file containing one or more values that we want
         to use to translate values  for.  using the dataframe's "lookup_column"
         as the key
        """
        # loop all columns
        for column in cols:
            # don't look at index column
            if column is not index_name:
                # read the incoming lookup filename into a dictionary using the
                # the appropriate columns to assign the dictionary key/value
                with open(filename) as f:
                    this_dict = dict((rows[cols.index(index_name)], rows[cols.index(column)]) \
                                     for rows in csv.reader(f))
                    # assign the new column name values based on lookup column name
                    data_frame[column] = data_frame[lookup_column].map(this_dict)
        return data_frame
