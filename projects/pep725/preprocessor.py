# -*- coding: utf-8 -*-


"""proprocessor.AbstractPreProcessor implementation for preprocessing pep725 data"""

import re, uuid
import pandas as pd
from preprocessor import AbstractPreProcessor

FILE_PREFIX = "pep725_"
COLUMNS_MAP = {
    'species': 'scientific_name',
    'day': 'day_of_year',
    'lat': 'latitude',
    'lon': 'longitude',
    'description': 'phenophase_name'
}
FILES = {
    'data': FILE_PREFIX + 'data.csv',
    'genus': FILE_PREFIX + 'genus.csv',
    'species': FILE_PREFIX + 'species.csv',
    'stations': FILE_PREFIX + 'stations.csv',
    'phase': FILE_PREFIX + 'phase.csv',
}


class PreProcessor(AbstractPreProcessor):
    def _process_data(self):
        self.frames = {
            'genus': pd.read_csv(self.input_dir + FILES['genus'], sep=';', header=0, usecols=['genus_id', 'genus'],
                                 skipinitialspace=True),
            'species': pd.read_csv(self.input_dir + FILES['species'], sep=';', header=0, skipinitialspace=True,
                                   usecols=['species_id', 'species']),
            'stations': pd.read_csv(self.input_dir + FILES['stations'], sep=';', header=0, skipinitialspace=True,
                                    usecols=['s_id', 'lat', 'lon']),  # , 'alt', 'name']),
            'phase': pd.read_csv(self.input_dir + FILES['phase'], sep=';', header=0,
                                 usecols=['phase_id', 'description'],
                                 skipinitialspace=True)
        }

        data = pd.read_csv(self.input_dir + FILES['data'], sep=';', header=0,
                           usecols=['s_id', 'genus_id', 'species_id', 'phase_id', 'year', 'day'], chunksize=100000,
                           skipinitialspace=True)

        for chunk in data:
            print("\tprocessing next {} records".format(len(chunk)))

            self._transform_data(chunk).to_csv(self.output_file, columns=self.headers, mode='a', header=False,
                                               index=False)
            return

    def _transform_data(self, data):
        joined_data = data \
            .merge(self.frames['species'], left_on='species_id', right_on='species_id', how='left') \
            .merge(self.frames['genus'], left_on='genus_id', right_on='genus_id', how='left') \
            .merge(self.frames['stations'], left_on='s_id', right_on='s_id', how='left') \
            .merge(self.frames['phase'], left_on='phase_id', right_on='phase_id', how='left')

        joined_data.fillna("", inplace=True)  # replace all null values

        joined_data = self._filter_data(joined_data)

        joined_data['record_id'] = joined_data.apply(lambda x: uuid.uuid4(), axis=1)
        joined_data['specificEpithet'] = joined_data.apply(
            lambda row: re.sub('^%s' % row['genus'], "", row['species']).strip(), axis=1)
        joined_data['source'] = 'PEP725'
        joined_data['lower_count'] = 1

        return joined_data.rename(columns=COLUMNS_MAP)

    @staticmethod
    def _filter_data(data):
        # manually remove some data from set that we don't want to work with
        # need to come up with a better method for this in the future!
        exclude = ["Beginning of seed imbibition, P, V: Beginning of bud swelling",
                   "D: Hypocotyl with cotyledons growing towards soil surface, P, V: Shoot growing towards soil surface",
                   "Dry seed (seed dressing takes place at stage 00), P, V: Winter dormancy or resting period",
                   "Elongation of radicle, formation of root hairs and /or lateral roots", "end of harvest",
                   "End of leaf fall, plants or above ground parts dead or dormant, P Plant resting or dormant",
                   "G: Coleoptile emerged from caryopsis, D, M: Hypocotyl with cotyledons or shoot breaking through seed coat, P, V: Beginning of sprouting or bud breaking",
                   "G: Emergence: Coleoptile breaks through soil surface, D, M: Emergence: Cotyledons break through soil surface(except hypogeal germination),D, V: Emergence: Shoot/leaf breaks through soil surface, P: Bud shows green tips",
                   "Grapevine bleeding, pruned grapes start to loss water from the cuts",
                   "Harvestable vegetative plant parts or vegetatively propagated organs begin to develop",
                   "Harvestable vegetative plant parts or vegetatively propagated organs have reached 30% of final size, G: Flag leaf sheath just visibly swollen (mid-boot)",
                   "Harvestable vegetative plant parts or vegetatively propagated organs have reached 50% of final size, G: Flag leaf sheath swollen (late-boot)",
                   "Harvestable vegetative plant parts or vegetatively propagated organs have reached 70% of final size, G: Flag leaf sheath opening",
                   "Harvestable vegetative plant parts or vegetatively propagated organs have reached final size, G: First awns visible, Skinset complete",
                   "Harvested product (post-harvest or storage treatment is applied at stage 99)",
                   "Maximum of total tuber mass reached, tubers detach easily from stolons, skin set not yet complete (skin easily removable with thumb)",
                   "P: Shoot development completed, foliage still green, grapevine: after harvest, end of wood maturation",
                   "Radicle (root) emerged from seed, P, V: Perennating organs forming roots",
                   "Seed imbibition complete, P, V: End of bud swelling", "Sowing", "start of harvest"]

        return data[~data['description'].isin(exclude)]
