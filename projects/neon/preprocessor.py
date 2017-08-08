# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing neon data

Also contains a few help functions that can be run directly from this script
"""

import pandas as pd

from xml.etree import ElementTree
from zipfile import ZipFile
from preprocessor import AbstractPreProcessor

from projects.neon.helpers import walk_files, INTENSITY_VALUE_FRAME

COLUMNS_MAP = {
    'uid': 'record_id',
    'dayOfYear': 'day_of_year',
    'scientificName': 'scientific_name',
    'phenophaseName': 'phenophase_name'
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        for file in walk_files(self.input_dir):
            print("\tprocessing {}".format(file))
            self._process_zip(file)

    def _process_zip(self, file):
        xml_file = None
        csv_file = None

        with ZipFile(file) as zip_file:
            for filename in zip_file.namelist():

                if filename.endswith('phe_statusintensity.csv'):
                    if csv_file:
                        raise RuntimeError('multiple csv files found in zip_file {}'.format(zip_file.filename))
                    csv_file = zip_file.open(filename)

                elif filename.endswith('.xml'):
                    if xml_file:
                        raise RuntimeError('multiple xml files found in zip_file {}'.format(zip_file.filename))
                    xml_file = zip_file.open(filename)

        if not csv_file or not xml_file:
            raise RuntimeError('missing xml or csv file in zip_file {}'.format(zip_file.filename))

        lat, lng = self._parse_coordinates(xml_file)

        data = pd.read_csv(csv_file, header=0, chunksize=100000, skipinitialspace=True,
                           usecols=['uid', 'date', 'dayOfYear', 'individualID', 'scientificName', 'phenophaseName',
                                    'phenophaseStatus', 'phenophaseIntensity'],
                           parse_dates=['date'])

        for chunk in data:
            self._transform_data(chunk, lat, lng).to_csv(self._out_file, columns=self.headers, mode='a', header=False,
                                                         index=False)

        csv_file.close()
        xml_file.close()

    @staticmethod
    def _transform_data(data, lat, lng):
        data['source'] = 'NEON'
        data['latitude'] = lat
        data['longitude'] = lng

        data['genus'] = data.apply(lambda row: row.scientificName.split()[0] if pd.notnull(row.scientificName) else "",
                                   axis=1)
        data['specific_epithet'] = data.apply(
            lambda row: row.scientificName.split()[1] if pd.notnull(row.scientificName) else "", axis=1)
        data['year'] = data.apply(lambda row: row['date'].year, axis=1)

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

        df["lower_count"] = df["lower_count"].fillna(0.0).astype(int)
        df["upper_count"] = df["upper_count"].fillna(0.0).astype(int)

        df.fillna('', inplace=True)  # replace all null values

        return df.rename(columns=COLUMNS_MAP)

    @staticmethod
    def _parse_coordinates(xml_file):
        tree = ElementTree.parse(xml_file)
        w_bound = tree.find('./dataset/coverage/geographicCoverage/boundingCoordinates/westBoundingCoordinate')
        e_bound = tree.find('./dataset/coverage/geographicCoverage/boundingCoordinates/eastBoundingCoordinate')
        n_bound = tree.find('./dataset/coverage/geographicCoverage/boundingCoordinates/northBoundingCoordinate')
        s_bound = tree.find('./dataset/coverage/geographicCoverage/boundingCoordinates/southBoundingCoordinate')

        if (w_bound is not None and e_bound is not None and w_bound.text != e_bound.text) or (
                            n_bound is not None and s_bound is not None and n_bound.text != s_bound.text):
            raise RuntimeError('bounding coordinates do not match for xml_file: {}'.format(xml_file.name))

        lat = n_bound.text if n_bound is not None else ""
        lng = w_bound.text if w_bound is not None else ""

        if not lat or not lng:
            raise RuntimeError('missing bounding coordinates for xml_file: {}'.format(xml_file.name))

        return lat, lng
