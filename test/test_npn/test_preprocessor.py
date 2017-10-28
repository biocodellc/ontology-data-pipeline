import pytest
import subprocess
import os

# This script expected to executed from the ppo-data-pipeline root
def test_entire_data_stream_from_input_to_output():
    output_file = 'output.txt'
    #with open(output_file + ".err", 'w') as output_err_file:
    # The ontology is hard-coded here so tests can pass even if ontology is changed.
    # The presence here of an up to data ontology is not necessary since we're just 
    # performing tests on pipeline functionality

    cmd = ['python', './process.py', 'test_npn', 'test/test_npn/data/output/', '--input_dir', 'test/test_npn/data/input/','--config_dir','test/test_npn/config/','--ontology','https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl','--base_dir','test/test_npn/','--project_base','test.test_npn' ]

    #proc = subprocess.Popen(cmd, stdout=output_err_file, stderr=subprocess.STDOUT)
    proc = subprocess.Popen(cmd)

    print("the commandline is {}".format(subprocess.list2cmdline(cmd)))

    stdout, stdin = proc.communicate()

    print (stdout)
    p_status = proc.wait()
   
    # Simple test to make sure the program exited with a good status
    assert p_status==0

    # TODO: look at output diretory and assert results

# should have an output_file path
#    assert True == os.path.exists(output_file)
