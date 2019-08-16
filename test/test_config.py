import pytest
import os
from process.config import Config


class Namespace:
    def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            
def test_config(tmpdir):
    base_dir = os.path.dirname(__file__)

    # Mimic the argument constructor from process:
    ns = Namespace(chunk_size=50000, config_dir='test/config', data_file=None, drop_invalid=True, input_dir='test/data/input', log_file=False, num_processes=4, ontology='test/test-ontology.owl', output_dir='test/data/output', preprocessor=None, project='test', project_base='projects', reasoner_config=None, split_data_column=None, verbose=True)

    # Build the Config class
    config = Config(**ns.__dict__)

    # Test that attributes are accessible
    assert config.output_dir == 'test/data/output'

    # verify that none existent attribute returns None
    assert config.doesnt_exist is None

    # verify rules were parsed 5 rules 
    assert len(config.rules) == 5
    # should split | delimited rule columns
    assert isinstance(config.rules[0]['columns'], list)
    # should assign default error level
    for rule in config.rules:
        assert rule['level'] in ["error", "warning"]

