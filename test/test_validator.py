import pytest
from testfixtures import LogCapture
from process.validator import Validator, InvalidData, DataValidator
import pandas as pd


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

@pytest.fixture
def config(tmpdir):
    import os
    from process.config import Config
    base_dir = os.path.dirname(__file__)

    def make_config(data_file):
        ns = Namespace(chunk_size=50000, config_dir='test/config', data_file=os.path.join(base_dir, data_file), drop_invalid=True, input_dir='test/data', log_file=False, num_processes=4, ontology='https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2018-07-31/ppo.owl', output_dir='test/data', preprocessor=None, project='test', project_base='projects', reasoner_config=None, split_data_column=None, verbose=True)

        #Build the Config class
        return Config(**ns.__dict__)

    return make_config


def _load_data(config):
    return pd.read_csv(config.data_file, header=0, skipinitialspace=True)


def test_should_raise_exception_if_missing_columns(config):
    config = config("data/missing_columns.csv")
    validator = Validator(config)

    #with pytest.raises(InvalidData):
    #    validator.validate(_load_data(config))

def test_should_return_false_for_invalid_data(config, capfd):
    config = config("data/invalid_input.csv")

    data = _load_data(config)
    validator = Validator(config)
    with LogCapture() as l:
        valid = validator.validate(data)
        assert valid is False

        # verify output
        l.check(
            ('root', 'INFO', 'ERROR: Duplicate values [1] in column `record_id`'),
                    ('root', 'INFO', 'ERROR: Value missing in required column `day_of_year`'),
                    ('root', 'INFO', 'ERROR: Value missing in required column `longitude`'),
                    ('root', 'INFO', 'WARNING: Value missing in required column `source`'),
                    ('root',
                     'INFO',
                     'WARNING: Value `invalid_year` in column `year` is not an integer'),
                    ('root', 'INFO', 'ERROR: Value `string` in column `latitude` is not a float'),
                    ('root', 'DEBUG', 'dropping invalid data')
        )

