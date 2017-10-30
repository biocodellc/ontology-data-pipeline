# -*- coding: utf-8 -*-
import logging
import subprocess


def convert_rdf2csv(input_dir, output_dir, sparql_file, query_fetcher_path):
    logging.debug("converting reasoned data to csv for directory {}".format(input_dir))

    cmd = ['java', '-jar', query_fetcher_path, '-i', input_dir, '-inputFormat', 'TURTLE', '-o', output_dir, '-numThreads', '8', '-sparql', sparql_file] 

    logging.debug("running query_fetcher with: ")
    logging.debug(subprocess.list2cmdline(cmd))

    subprocess.run(cmd, check=True)

    # with process.stdout:
    #     for line in iter(process.stdout.readline, b''):
    #         logging.debug(line)
    
    # write obo: prefix to all obo URLs using inplace editing.
    # This cuts the output file sizes by 50% but we will need 
    # to remember to replace the prefix in any downstream apps
    with fileinput.FileInput('foo.txt', inplace=True) as file:
        for line in file:
            print(line.replace('http://purl.obolibrary.org/obo/', 'obo:'), end='')

    fileinput.close()
