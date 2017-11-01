#!/bin/sh
python ./process.py test_neon test_data/data/test_neon/output --input_dir test_data/data/test_neon/input --config_dir test_data/config --ontology https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl --base_dir test_data/projects/test_neon --project_base test_data.projects.test_neon --verbose
