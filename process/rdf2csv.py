# -*- coding: utf-8 -*-
import logging
import subprocess
import fileinput
import os
import re


def convert_rdf2csv(input_file, output_dir, sparql_file, query_fetcher_path):
    logging.debug("converting reasoned data to csv for file {}".format(input_file))

    cmd = ['java', '-jar', query_fetcher_path, '-i', input_file, '-inputFormat', 'TURTLE', '-o', output_dir, '-sparql', sparql_file] 

    logging.debug("running query_fetcher with: ")
    logging.debug(subprocess.list2cmdline(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    # take filename from stdout from process, decode and clean up output
    filename = str(stdout.decode('utf-8')).replace('writing ','').replace('\n','').lstrip()

    if not os.path.isfile(filename):
        raise RuntimeError("Could not find output file from query_fetcher.  You can isolate the process and debug using: " +subprocess.list2cmdline(cmd))


    # with process.stdout:
    #     for line in iter(process.stdout.readline, b''):
    #         logging.debug(line)
    
    # write obo: prefix to all obo URLs using inplace editing.
    # This cuts the output file sizes by 50% but we will need 
    # to remember to replace the prefix in any downstream apps
    with fileinput.FileInput(filename, inplace=True) as file:
        for line in file:
            print(line.replace('http://purl.obolibrary.org/obo/', 'obo:'), end="");
            #line.replace('http://purl.obolibrary.org/obo/', 'obo:')

    fileinput.close()


    logging.info(stdout)

    # provide docker friendly output (this way user looks for file in relative path home environment instead of docker mount)
    cleanfilename = re.sub('^%s' % '/process/', '', filename)
    logging.info('reasoned_csv output at ' + cleanfilename)
