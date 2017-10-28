import pytest
import os
from process.config import Config


def test_config(tmpdir):
    base_dir = os.path.dirname(__file__)
    config = Config( {
        'output_dir': str(tmpdir),
        'base_dir': os.path.join(base_dir, "."),
        'data_file': os.path.join(base_dir, "data/invalid_input.csv"),
        'config_dir': os.path.join(base_dir, "config")
    }, kw=False)

    # verify that passed in args & kwargs are accessible as attributes
    assert config.kw == False
    assert config.output_dir == tmpdir

    # should setup some attributes
    # These two failing turning off for now
    assert config.invalid_data_file == tmpdir.join('invalid_data.csv')

    # verify that none existent attribute returns None
    assert config.doesnt_exist is None

    # verify rules were parsed 5 rules + 1 default
    assert len(config.rules) == 6
    # should split | delimited rule columns
    assert isinstance(config.rules[0]['columns'], list)
    # should assign default error level
    for rule in config.rules:
        assert rule['level'] in ["error", "warning"]

    # should parse phenophase_descriptions file
    descriptions = config.lists['phenophase_descriptions.csv']
    assert {'field': 'Reproductive', 'defined_by': 'http://purl.obolibrary.org/obo/PPO_0002025'} in descriptions
    assert {'field': 'Flowering', 'defined_by': 'http://purl.obolibrary.org/obo/PPO_0002035'} in descriptions
    assert {'field': 'Fruiting', 'defined_by': 'http://purl.obolibrary.org/obo/PPO_0002045'} in descriptions

    # should be 3 valid phenophase_descriptions list items
    assert len(descriptions) == 3
    assert len(config.lists['phenophase_descriptions.csv']) == 3

    # should parse entities and perform label substitution
    assert {
            'alias': 'plantStructurePresence',
            'concept_uri': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'unique_key': 'record_id',
            'identifier_root': 'http://n2t.net/ark:/21547/Anl2',
            'columns': [('phenophase_name', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')]
        } in config.entities
    assert {
            'alias': 'phenologicalObservingProcess',
            'concept_uri': 'http://purl.obolibrary.org/obo/BCO_0000003',
            'unique_key': 'record_id',
            'identifier_root': 'http://n2t.net/ark:/21547/Anm2',
            'columns': [('record_id', 'http://rs.tdwg.org/dwc/terms/EventID'), ('latitude', 'http://rs.tdwg.org/dwc/terms/decimalLatitude'),
                        ('longitude', 'http://rs.tdwg.org/dwc/terms/decimalLongitude'), ('year', 'http://rs.tdwg.org/dwc/terms/year'),
                        ('day_of_year', 'http://rs.tdwg.org/dwc/terms/startDayOfYear'), ('source', 'http://purl.org/dc/elements/1.1/creator')]
        } in config.entities

    assert len(config.entities) == 2

    # should parse relations and perform label substitution
    assert {
            'subject_entity_alias': 'plantStructurePresence',
            'predicate': 'http://purl.obolibrary.org/obo/OBI_0000295',
            'object_entity_alias': 'phenologicalObservingProcess'
        } in config.relations
