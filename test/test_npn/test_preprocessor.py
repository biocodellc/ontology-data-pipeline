import pytest
import subprocess 
import os

# This script expected to executed from the ppo-data-pipeline root
def test_entire_data_stream_from_input_to_output():
    base_dir = os.path.dirname(__file__)
    output_file = 'output.txt'

    #with open(output_file + ".err", 'w') as output_err_file:
    # The ontology is hard-coded here so tests can pass even if ontology is changed.
    # The presence here of an up to data ontology is not necessary since we're just 
    # performing tests on pipeline functionality

    cmd = ['python', './process.py', 'test_npn', 'test/test_npn/data/output/', '--input_dir', 'test/test_npn/data/input/','--config_dir','test/test_npn/config/','--ontology','https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl','--base_dir','test/test_npn/','--project_base','test.test_npn' ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    #print("the commandline is {}".format(subprocess.list2cmdline(cmd)))

    # output reresults from process
    stdout, stderr = proc.communicate()
    out = open(output_file,'wb')
    out.write(stdout)
    out.close() 
    err = open(output_file + '.err','wb')
    err.write(stderr)
    err.close() 

    p_status = proc.wait()
    # Simple test to make sure the program exited with a good status
    assert p_status==0

    # should have a file with results
    results_file_name = os.path.join(base_dir,"data","output","output_csv","data.csv")
    assert True == os.path.exists(results_file_name)

    expected_results = [
            'http://n2t.net/ark:/21547/Amg24,139,2009,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,0,,NPN,,16.917099,http://purl.obolibrary.org/obo/PPO_0002041|http://purl.obolibrary.org/obo/PPO_0002000',
            'http://n2t.net/ark:/21547/Amg23,141,2011,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,75.0,94.0,NPN,,14.151621,http://purl.obolibrary.org/obo/PPO_0002314|http://purl.obolibrary.org/obo/PPO_0002013|http://purl.obolibrary.org/obo/PPO_0002312|http://purl.obolibrary.org/obo/PPO_0002302|http://purl.obolibrary.org/obo/PPO_0002016|http://purl.obolibrary.org/obo/PPO_0002011|http://purl.obolibrary.org/obo/PPO_0002014|http://purl.obolibrary.org/obo/PPO_0002322|http://purl.obolibrary.org/obo/PPO_0002024|http://purl.obolibrary.org/obo/PPO_0002313|http://purl.obolibrary.org/obo/PPO_0002301|http://purl.obolibrary.org/obo/PPO_0002003|http://purl.obolibrary.org/obo/PPO_0002004|http://purl.obolibrary.org/obo/PPO_0002009|http://purl.obolibrary.org/obo/PPO_0002311|http://purl.obolibrary.org/obo/PPO_0002309|http://purl.obolibrary.org/obo/PPO_0002000|http://purl.obolibrary.org/obo/PPO_0002015|http://purl.obolibrary.org/obo/PPO_0002307',
            'http://n2t.net/ark:/21547/Amg21,139,2011,43.08535,-70.69133,Acer,rubrum,Acer rubrum,11.0,,,,NPN,,14.151621,http://purl.obolibrary.org/obo/PPO_0002314|http://purl.obolibrary.org/obo/PPO_0002312|http://purl.obolibrary.org/obo/PPO_0002302|http://purl.obolibrary.org/obo/PPO_0002013|http://purl.obolibrary.org/obo/PPO_0002016|http://purl.obolibrary.org/obo/PPO_0002014|http://purl.obolibrary.org/obo/PPO_0002322|http://purl.obolibrary.org/obo/PPO_0002313|http://purl.obolibrary.org/obo/PPO_0002011|http://purl.obolibrary.org/obo/PPO_0002301|http://purl.obolibrary.org/obo/PPO_0002003|http://purl.obolibrary.org/obo/PPO_0002024|http://purl.obolibrary.org/obo/PPO_0002004|http://purl.obolibrary.org/obo/PPO_0002009|http://purl.obolibrary.org/obo/PPO_0002311|http://purl.obolibrary.org/obo/PPO_0002309|http://purl.obolibrary.org/obo/PPO_0002000|http://purl.obolibrary.org/obo/PPO_0002015|http://purl.obolibrary.org/obo/PPO_0002307',
            'http://n2t.net/ark:/21547/Amg26,145,2009,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,,0,NPN,,16.917099,http://purl.obolibrary.org/obo/PPO_0002035|http://purl.obolibrary.org/obo/PPO_0002000',
            'http://n2t.net/ark:/21547/Amg25,145,2009,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,1.0,,NPN,,16.917099,http://purl.obolibrary.org/obo/PPO_0002312|http://purl.obolibrary.org/obo/PPO_0002015|http://purl.obolibrary.org/obo/PPO_0002315|http://purl.obolibrary.org/obo/PPO_0002017|http://purl.obolibrary.org/obo/PPO_0002000|http://purl.obolibrary.org/obo/PPO_0002313|http://purl.obolibrary.org/obo/PPO_0002014',
            'http://n2t.net/ark:/21547/Amg27,145,2009,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,51.0,100.0,NPN,,16.917099,http://purl.obolibrary.org/obo/PPO_0002318|http://purl.obolibrary.org/obo/PPO_0002320|http://purl.obolibrary.org/obo/PPO_0002312|http://purl.obolibrary.org/obo/PPO_0002020|http://purl.obolibrary.org/obo/PPO_0002322|http://purl.obolibrary.org/obo/PPO_0002315|http://purl.obolibrary.org/obo/PPO_0002015|http://purl.obolibrary.org/obo/PPO_0002000|http://purl.obolibrary.org/obo/PPO_0002017|http://purl.obolibrary.org/obo/PPO_0002018|http://purl.obolibrary.org/obo/PPO_0002022|http://purl.obolibrary.org/obo/PPO_0002316|http://purl.obolibrary.org/obo/PPO_0002024|http://purl.obolibrary.org/obo/PPO_0002313|http://purl.obolibrary.org/obo/PPO_0002014',
            'http://n2t.net/ark:/21547/Amg22,139,2011,43.08535,-70.69133,Acer,rubrum,Acer rubrum,,,1.0,,NPN,,14.151621,http://purl.obolibrary.org/obo/PPO_0002322|http://purl.obolibrary.org/obo/PPO_0002313|http://purl.obolibrary.org/obo/PPO_0002000|http://purl.obolibrary.org/obo/PPO_0002301|http://purl.obolibrary.org/obo/PPO_0002003|http://purl.obolibrary.org/obo/PPO_0002004|http://purl.obolibrary.org/obo/PPO_0002009|http://purl.obolibrary.org/obo/PPO_0002311|http://purl.obolibrary.org/obo/PPO_0002013|http://purl.obolibrary.org/obo/PPO_0002011|http://purl.obolibrary.org/obo/PPO_0002024|http://purl.obolibrary.org/obo/PPO_0002015|http://purl.obolibrary.org/obo/PPO_0002307|http://purl.obolibrary.org/obo/PPO_0002314|http://purl.obolibrary.org/obo/PPO_0002312|http://purl.obolibrary.org/obo/PPO_0002302|http://purl.obolibrary.org/obo/PPO_0002016|http://purl.obolibrary.org/obo/PPO_0002309|http://purl.obolibrary.org/obo/PPO_0002014'
     ]

    # compare expected values above to results_file
    # first, read results_file into an array of lines
    results_file = open(results_file_name,'r')
    lines = results_file.read().splitlines()
    results_file.close()

    # loop expected results and search for matching lines in output
    for e in expected_results:
        foundMatch = False
        # loop actual output lines
        for line in lines:
            # search for the expected line in the output
            if (e == line):
                foundMatch = True
        if (foundMatch):
            assert True
        else:
            assert False, "expected line did not find a match: " + e

    # we should see the same number for results in output file as
    # the expected, with the exception of the header line
    assert len(expected_results) == len(lines)-1, "mismatched number of lines"
