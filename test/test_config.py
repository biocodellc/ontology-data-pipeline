import os
from process.config import Config


def test_config(tmpdir):
    base_dir = os.path.dirname(__file__)
    config = Config(base_dir, {
        'output_dir': tmpdir,
        'data_file': os.path.join(base_dir, "data/invalid_input.csv"),
        # 'ontology': os.path.join(base_dir, "config/ppo-reasoned-no-imports.owl"),
    }, kw=False)

    # verify that passed in args & kwargs are accessible as attributes
    assert config.kw == False
    assert config.output_dir == tmpdir

    # should setup some attributes
    assert config.invalid_data_file.name == tmpdir.join('invalid_data.csv')
    assert not config.invalid_data_file.closed

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
    assert config.pheno_descriptions == {
        'Reproductive': '{reproductive structure presence}',
        'Flowering': '{open flower presence}',
        'Fruiting': '{ripening fruit presence}',
    }

    # should be 3 valid phenophase_names list items
    assert len(config.lists['phenophase_names']) == 3

    # should parse entities and perform label substitution
    assert config.entities == [
        {
            'alias': 'plantStructurePresence',
            'concept_uri': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'unique_key': 'record_id',
            'identifier': 'http://n2t.net/ark:/21547/Anl2'
        },
        {
            'alias': 'phenologicalObservingProcess',
            'concept_uri': 'http://purl.obolibrary.org/obo/BCO_0000003"',
            'unique_key': 'record_id',
            'identifier': 'http://n2t.net/ark:/21547/Anm2'
        }
    ]

    # should parse relations and perform label substitution
    assert config.entities == [
        {
            'subject_entity_alias': 'plantStructurePresence',
            'predicate': 'http://purl.obolibrary.org/obo/OBI_0000295',
            'object_entity_alias': 'phenologicalObservingProcess'
        }
    ]
