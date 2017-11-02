# -*- coding: utf-8 -*-
import logging
import subprocess
import fileinput
import os


def convert_rdf2csv(input_file, output_dir, sparql_file, query_fetcher_path):
    logging.debug("converting reasoned data to csv for file {}".format(input_file))

    cmd = ['java', '-jar', query_fetcher_path, '-i', input_file, '-inputFormat', 'TURTLE', '-o', output_dir, '-numThreads', '1', '-sparql', sparql_file] 

    logging.debug("running query_fetcher with: ")
    logging.debug(subprocess.list2cmdline(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    # take filename from stdout from process, decode and clean up output
    filename = str(stdout.decode('utf-8')).replace('writing ','').replace('\n','').lstrip()

    if not os.path.isfile(filename):
        raise RuntimeError("Failed to find filename from query_fetcher {}".format(filename))

    # with process.stdout:
    #     for line in iter(process.stdout.readline, b''):
    #         logging.debug(line)
    
    # write obo: prefix to all obo URLs using inplace editing.
    # This cuts the output file sizes by 50% but we will need 
    # to remember to replace the prefix in any downstream apps
    with fileinput.FileInput(filename, inplace=True) as file:
        for line in file:
            print(line.replace('http://purl.obolibrary.org/obo/', 'obo:'), end='')

    fileinput.close()

    logging.info(stdout)
