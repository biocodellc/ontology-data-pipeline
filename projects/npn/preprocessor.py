# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing npn data"""

import os, csv
import pandas as pd
from preprocessor import AbstractPreProcessor


PHENOPHASE_DESCRIPTIONS_FILE = os.path.join(os.path.dirname(__file__), 'phenophase_descriptions.csv')
COLUMNS_MAP = {
    'Observation_ID': 'record_id',
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'Day_of_Year': 'day_of_year',
    'Genus': 'genus',
    'Species': 'specific_epithet',
    'Phenophase_Description': 'phenophase_name',
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        self.descriptions = pd.read_csv(PHENOPHASE_DESCRIPTIONS_FILE, header=0, skipinitialspace=True)
        # Loop each directory off of input directory
        for dirname in os.listdir(self.input_dir):

            # make sure we're just dealing with the proper directories
            if dirname.startswith('datasheet_') and dirname.endswith("zip") is False:

                dirname = os.path.join(self.input_dir, dirname)

                # loop all filenames in directory
                onlyfiles = [f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

                for filename in onlyfiles:

                    # phenology data frame
                    if filename == 'status_intensity_observation_data.csv':
                        chunk_size = 100000
                        tp = pd.read_csv(os.path.join(dirname, filename), sep=',', header=0, iterator=True,
                                         chunksize=chunk_size, dtype=object)

                        for df in tp:
                            print("\tprocessing next {} records".format(len(df)))
                            self._transform_data(df).to_csv(self.output_file, columns=self.headers, mode='a',
                                                            header=False, index=False)

    def _transform_data(self, df):
        # Add an index name
        df.index.name = 'record_id'

        # Translate values from intensity_values.csv file

        # translate values
        cols = ['value', 'lower_count', 'upper_count', 'lower_percent', 'upper_percent']
        df = self._translate(os.path.join(os.path.dirname(__file__), 'intensity_values.csv'), cols, 'value', df,
                              'Intensity_Value')

        # set upper/lower counts for cases of no intensity value
        df.apply(lambda row: self._set_defaults(row), axis=1)

        df['source'] = 'NPN'

        # Normalize Date to just Year. we don't need to store actual date because we use only Year + DayOfYear
        df['year'] = pd.DatetimeIndex(df['Observation_Date']).year

        # Create ScientificName
        df['scientific_name'] = df['Genus'] + ' ' + df['Species']

        # drop duplicate ObservationIDs
        df.drop_duplicates('Observation_ID', inplace=True)

        return df.rename(columns=COLUMNS_MAP)

    def _set_defaults(self, row):
        if row.Intensity_Value != '-9999':
            return row

        try:
            if row.Phenophase_Status == '0':
                row['lower_percent'] = self.descriptions[self.descriptions['field'] == row['Phenophase_Description']]['lower_percent_absent'].values[0]
                row['upper_percent'] = self.descriptions[self.descriptions['field'] == row['Phenophase_Description']]['upper_percent_absent'].values[0]
            else:
                row['lower_percent'] = self.descriptions[self.descriptions['field'] == row['Phenophase_Description']]['lower_percent_present'].values[0]
                row['upper_percent'] = self.descriptions[self.descriptions['field'] == row['Phenophase_Description']]['upper_percent_present'].values[0]
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
