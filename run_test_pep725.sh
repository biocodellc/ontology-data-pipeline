#!/bin/sh
python ./process.py test_pep725 test_data/data/test_pep725/output --input_dir test_data/data/test_pep725/input --config_dir test_data/config --ontology https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl --base_dir test_data/projects/test_pep725 --project_base test_data.projects.test_pep725 --verbose
