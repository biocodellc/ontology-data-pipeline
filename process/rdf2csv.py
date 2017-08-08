# -*- coding: utf-8 -*-
import logging
import subprocess


def convert_rdf2csv(input_dir, output_dir, sparql_file, query_fetcher_path):
    logging.debug("converting reasoned data to csv for directory {}".format(input_dir))

    process = subprocess.run(['java', '-jar', query_fetcher_path,
                              '-i', input_dir,
                              '-inputFormat', 'TURTLE',
                              '-o', output_dir,
                              '-numThreads', '8',
                              '-sparql', sparql_file],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             check=True)
    logging.debug(process.communicate())
