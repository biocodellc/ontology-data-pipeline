import pytest
import pandas as pd

from process.triplifier import Triplifier


@pytest.fixture
def config(tmpdir):
    import os
    from process.config import Config
    base_dir = os.path.dirname(__file__)

    def make_config(data_file):
        return Config({
            'output_dir': str(tmpdir),
            'base_dir': os.path.join(base_dir, "."),
            'data_file': os.path.join(base_dir, data_file),
            'config_dir': os.path.join(base_dir, "config"),
            'ontology': os.path.join(base_dir, 'config/ppo-reasoned-no-imports.owl')
        })

    return make_config


def _load_data(config):
    return pd.read_csv(config.data_file, header=0, skipinitialspace=True)


def test_should_generate_valid_triples(config):
    config = config("data/valid.csv")
    triplifier = Triplifier(config)

    triples = triplifier.triplify(_load_data(config))

    expected_triples = [
        '<http://n2t.net/ark:/21547/Anl21> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/PPO_0002022>',
        '<http://n2t.net/ark:/21547/Anm21> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/BCO_0000003>',
        '<http://n2t.net/ark:/21547/Anm21> <http://rs.tdwg.org/dwc/terms/EventID> "1"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '<http://n2t.net/ark:/21547/Anm21> <http://rs.tdwg.org/dwc/terms/decimalLatitude> "-12.99"^^<http://www.w3.org/2001/XMLSchema#float>',
        '<http://n2t.net/ark:/21547/Anm21> <http://rs.tdwg.org/dwc/terms/decimalLongitude> "13.0"^^<http://www.w3.org/2001/XMLSchema#float>',
        '<http://n2t.net/ark:/21547/Anm21> <http://rs.tdwg.org/dwc/terms/year> "1988"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '<http://n2t.net/ark:/21547/Anm21> <http://rs.tdwg.org/dwc/terms/startDayOfYear> "120"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '<http://n2t.net/ark:/21547/Anm21> <http://purl.org/dc/elements/1.1/creator> "me"^^<http://www.w3.org/2001/XMLSchema#string>',
        '<http://n2t.net/ark:/21547/Anl21> <http://purl.obolibrary.org/obo/OBI_0000295> <http://n2t.net/ark:/21547/Anm21>',
        '<http://purl.obolibrary.org/obo/OBI_0000295> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>',
        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
        '<http://purl.obolibrary.org/obo/BCO_0000003> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class>',
        '<http://rs.tdwg.org/dwc/terms/EventID> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://rs.tdwg.org/dwc/terms/EventID> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://rs.tdwg.org/dwc/terms/EventID> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://rs.tdwg.org/dwc/terms/EventID>',
        '<http://rs.tdwg.org/dwc/terms/decimalLatitude> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://rs.tdwg.org/dwc/terms/decimalLatitude> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://rs.tdwg.org/dwc/terms/decimalLatitude> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://rs.tdwg.org/dwc/terms/decimalLatitude>',
        '<http://rs.tdwg.org/dwc/terms/decimalLongitude> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://rs.tdwg.org/dwc/terms/decimalLongitude> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://rs.tdwg.org/dwc/terms/decimalLongitude> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://rs.tdwg.org/dwc/terms/decimalLongitude>',
        '<http://rs.tdwg.org/dwc/terms/year> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://rs.tdwg.org/dwc/terms/year> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://rs.tdwg.org/dwc/terms/year> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://rs.tdwg.org/dwc/terms/year>',
        '<http://rs.tdwg.org/dwc/terms/startDayOfYear> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://rs.tdwg.org/dwc/terms/startDayOfYear> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://rs.tdwg.org/dwc/terms/startDayOfYear> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://rs.tdwg.org/dwc/terms/startDayOfYear>',
        '<http://purl.org/dc/elements/1.1/creator> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>',
        '<http://purl.org/dc/elements/1.1/creator> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty>',
        '<http://purl.org/dc/elements/1.1/creator> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://purl.org/dc/elements/1.1/creator>'
    ]

    # loop the found triples in the expected triples with special logic to handle importantInstance triples, which are system
    # specific.  We need to know we found just one important instance and don't compare that line here
    foundImportInstance = False
    for t in triples:
        if t.startswith('<urn:importInstance>'): 
            assert foundImportInstance is False
            foundImportInstance = True
        else: 
            assert t in expected_triples

    assert foundImportInstance is True

    # we removed the import instance since it is typically system specific from the expected triples so we need to add one more line
    assert len(triples) == len(expected_triples)+1
