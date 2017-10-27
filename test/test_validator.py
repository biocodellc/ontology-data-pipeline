import pytest
from process.validator import Validator, InvalidData, DataValidator
import pandas as pd


@pytest.fixture
def config(tmpdir):
    import os
    from process.config import Config
    base_dir = os.path.dirname(__file__)

    def make_config(data_file):
        return Config(base_dir, {
            'output_dir': str(tmpdir),
            'data_file': os.path.join(base_dir, data_file),
            'config_dir': os.path.join(base_dir, "config")

        })

    return make_config


def _load_data(config):
    return pd.read_csv(config.data_file, header=0, skipinitialspace=True)


def test_should_raise_exception_if_missing_columns(config):
    config = config("data/missing_columns.csv")
    #validator = Validator(config, _load_data(config))
    validator = Validator(config)

    with pytest.raises(InvalidData):
        validator.validate(_load_data(config))

def test_should_return_false_for_invalid_data(config, capsys):
    config = config("data/invalid_input.csv")

    with capsys.disabled():
        data = _load_data(config)
        validator = Validator(config)
        valid = validator.validate(data)

    assert valid == False

    # verify output
    out, err = capsys.readouterr()
    assert out == "ERROR: Duplicate values [1] in column `record_id`\n" + \
                  "ERROR: Value missing in required column `day_of_year`\n" + \
                  "ERROR: Value missing in required column `longitude`\n" + \
                  "WARNING: Value missing in required column `source`\n" + \
                  "WARNING: Value `invalid_year` in column `year` is not an integer\n" + \
                  "ERROR: Value `string` in column `latitude` is not a float\n" + \
                  "ERROR: Value `invalid_name` in column `phenophase_name` is not in the controlled vocabulary list `phenophase_descriptions.csv`\n"

    # verify invalid_data_file contents
    config.invalid_data_file.close()
    i_data = pd.read_csv(config.invalid_data_file.name, skipinitialspace=True)
    assert i_data['record_id'].equals(data['record_id'])
