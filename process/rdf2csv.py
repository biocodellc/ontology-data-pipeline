# -*- coding: utf-8 -*-
import logging
import subprocess
import fileinput
import os
import re


def convert_rdf2csv(input_file, output_dir, sparql_file, robot_path):
    logging.debug("converting reasoned data to csv for file {}".format(input_file))
    input_filename = os.path.basename(input_file)
    output_pathfile = os.path.join(output_dir,input_filename+'.csv')
    cmd = ['java', '-Xmx6048m', '-jar', robot_path, 'query','--input', input_file, '--query', sparql_file , output_pathfile] 

    logging.debug("running SPARQL query with the following robot command: ")
    logging.debug(subprocess.list2cmdline(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    # take filename from stdout from process, decode and clean up output
    #filename = str(stdout.decode('utf-8')).replace('writing ','').replace('\n','').lstrip()

    if not os.path.isfile(output_pathfile):
        #logging.info("Error message: " + stderr)
        raise RuntimeError("Could not find output file from robot.  You can isolate the process and debug using: " +subprocess.list2cmdline(cmd))


    # write obo: prefix to all obo URLs using inplace editing.
    # This cuts the output file sizes by 50% but we will need 
    # to remember to replace the prefix in any downstream apps
    with fileinput.FileInput(output_pathfile, inplace=True) as file:
        for line in file:
            print(line.replace('http://purl.obolibrary.org/obo/', 'obo:'), end="");
    with fileinput.FileInput(output_pathfile, inplace=True) as file:
        for line in file:
            print(line.replace('https://purl.obolibrary.org/obo/', 'obo:'), end="");

    fileinput.close()


    logging.info(stdout)

    # provide docker friendly output (this way user looks for file in relative path home environment instead of docker mount)
    cleanfilename = re.sub('^%s' % '/process/', '', output_pathfile)
    logging.info('reasoned_csv output at ' + cleanfilename)
