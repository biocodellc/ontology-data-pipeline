#!/bin/sh
python ./process.py test_npn test/test_npn/data/output/ --input_dir test/test_npn/data/input/ --config_dir test/test_npn/config/ --ontology https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl --base_dir test/test_npn/ --project_base test.test_npn --verbose
