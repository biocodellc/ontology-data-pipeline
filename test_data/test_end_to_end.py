import pytest
import subprocess 
import os


# This script expected to executed from the ppo-data-pipeline root
#@pytest.mark.parametrize("project", [ "npn", "pep725"]) 
@pytest.mark.parametrize("project", [ "npn","neon"]) 
def test_end_to_end(project):
    #base_dir = os.path.dirname(__file__)

    # set the project, always preceeded with test_
    project_name = 'test_'+project
    # set the project base dir
    project_base_dir = os.path.join('test_data','projects') 
    # set the project base, based on project name
    project_base = 'test_data'+'.projects.'+project_name #python name reference for dynamic class loading
    # set the project output path
    project_path = os.path.join(project_base_dir,project_name)
    # output directory
    output_path = os.path.join('test_data','data',project_name,'output')
    # input directory
    input_path = os.path.join('test_data','data',project_name,'input')
    # path pointing to onfiguration files. we do not use the main project configuration directory 
    # for the application itself as that may contain changes we don't wish to test.
    # to test changes in configuration files the relevant config files should be copied here
    config_path = os.path.join('test_data','config')
    # reference to ontology. Do NOT change this as it will interfere with rest results. it is Okay if ontology is
    # out of date.  Here we reference a specific release of the ontology itself so it should be static
    ontology_url = 'https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl'
    # file containing actual results we want to compare to
    actual_results_file_name = os.path.join(output_path,"output_reasoned_csv","data_1.ttl.csv")
    # file containing expected results
    expected_results_file_name = os.path.join('test_data','test_'+project+'_results.txt')
    # name of file to store output text, if test fails we can learn more information in this file
    output_file = 'output.txt'

    # The ontology is hard-coded here so tests can pass even if ontology is changed.
    # The presence here of an up to data ontology is not necessary since we're just 
    # performing tests on pipeline functionality
    cmd = ['python', './process.py', project_name, output_path, '--input_dir', input_path,'--config_dir',config_path,'--ontology',ontology_url,'--base_dir',project_path,'--project_base',project_base ]

    # setup process to execute given command
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    print("")
    print("the commandline is")
    print(subprocess.list2cmdline(cmd))

    # output reresults from process
    stdout, stderr = proc.communicate()
    out = open(output_file,'wb')
    out.write(stdout)
    out.close() 
    err = open(output_file+ '.err','wb')
    err.write(stderr)
    err.close() 

    # wait for process to complete before continuing
    p_status = proc.wait()

    # Simple test to make sure the program exited with a good status
    assert p_status==0

    # Read actual results, stored as results_file into an array of lines
    assert True == os.path.exists(actual_results_file_name)
    actual_results_file = open(actual_results_file_name,'r')
    actual_results = actual_results_file.read().splitlines()
    actual_results_file.close()

    # Read expected results, stored as results_file into an array of lines
    assert True == os.path.exists(expected_results_file_name)
    expected_results_file = open(expected_results_file_name,'r')
    expected_results = expected_results_file.read().splitlines()
    expected_results_file.close()

    # loop expected results and search for matching lines in output
    for e_line in expected_results:
        foundMatch = False
        # loop actual output lines
        for a_line in actual_results:
            # search for the expected line in the output
            #if (e.replace(r"\'","'") == line):
            if (e_line == a_line):
                foundMatch = True
        if (foundMatch):
            assert True
        else:
            assert False, "expected line did not find a match: " + e

    # we should see the same number for results in output file as
    # the expected, with the exception of the header line
    assert len(expected_results) == len(actual_results)-1, "mismatched number of lines"
