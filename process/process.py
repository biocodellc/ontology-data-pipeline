# -*- coding: utf-8 -*-

import argparse
import logging
import os
import math

import multiprocessing
from itertools import repeat

import numpy
import pandas as pd

from .rdf2csv import convert_rdf2csv
from .splitter import split_file
from .utils import  loadClass, clean_dir
from .config import Config
from .reasoner import run_reasoner
from .triplifier import Triplifier
from .validator import Validator


"""process.Process: provides entry point main()."""
__version__ = "0.1.0"



class Process(object):
    def __init__(self, config):
        self.config = config

        self.triplifier = Triplifier(config)
        self.validator = Validator(config)

    def run(self):

        # empty output directories
        clean_dir(self.config.output_unreasoned_dir)
        clean_dir(self.config.output_reasoned_dir)

        self._triplify_all()
        self._reason_all()

        if self.config.reasoned_sparql_exists:
            clean_dir(self.config.output_reasoned_csv_dir)
            self._csv2rdf_all()
        else:
            logging.warning("Skipping rdf2csv conversion, no SPARQL query found.")

    def _reason_all(self):
        num_processes = math.floor(self.config.num_processes / 2)
        if (num_processes < 1):
            num_processes = 1

        for root, dirs, files in os.walk(self.config.output_unreasoned_dir):
            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.starmap(self._reason, zip(files, repeat(root)))

    def _csv2rdf(self, file):
        logging.debug("\trunning csv2reasoner on {}".format(file))
        out_file = os.path.join(self.config.output_reasoned_dir, file.replace('.n3', '.ttl'))
        convert_rdf2csv(os.path.join(self.config.output_reasoned_dir,file),self.config.output_reasoned_csv_dir,
                        self.config.reasoned_sparql, self.config.queryfetcher)

    def _csv2rdf_all(self):
        num_processes = math.floor(self.config.num_processes / 2)
        files = []
        for file in os.listdir(self.config.output_reasoned_dir):
            if file.endswith(".ttl"):
                files.append(file)

        if (num_processes < 1):
            num_processes = 1

        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(self._csv2rdf, zip(files))

    def _triplify_all(self):
        # check for incoming data file before triplifying
        if not os.path.exists(self.config.data_file):
            raise RuntimeError("cannot find input datafile "+ self.config.data_file)
        
        num_processes = self.config.num_processes
        data = pd.read_csv(self.config.data_file, header=0, skipinitialspace=True,
                           chunksize=self.config.chunk_size * num_processes)

        i = 1
        for data_frame in data:
            logging.debug("\tvalidating {} records".format(len(data_frame)))

            # parallelize the process
            # https://stackoverflow.com/questions/40357434/pandas-df-iterrow-parallelization
            chunk_size = self.config.chunk_size

            # this solution was reworked from the above link.
            # will work even if the length of the dataframe is not evenly divisible by num_processes
            chunks = [data_frame.ix[data_frame.index[i:i + chunk_size]] for i in
                      range(0, data_frame.shape[0], chunk_size)]

            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.starmap(self._triplify_chunk, zip(chunks, [n for n in range(i, i + len(chunks) + 1)]))

            i += len(chunks)

    def _triplify_chunk(self, chunk, i):
        triples_file = os.path.join(self.config.output_unreasoned_dir, "data_{}.ttl".format(i))
        self._triplify(chunk, triples_file)

    def _triplify(self, data, triples_file):
        valid = self.validator.validate(data)

        if not valid:
            invalid_data_path = os.path.realpath(self.config.invalid_data_file)
            if self.config.log_file:
                log_path = os.path.realpath(self.config.log_file)
                print("Validation Failed! The logs are located {} and {}".format(invalid_data_path, log_path))
            else:
                print("Validation Failed! The logs are located {}".format(invalid_data_path))
            return

        logging.debug("\ttriplifying {} records".format(len(data)))

        triples = self.triplifier.triplify(data)

        with open(triples_file, 'w') as f:
            for t in triples:
                f.write("{} .\n".format(t))

    def _reason(self, file, root):
        logging.debug("\trunning reasoner on {}".format(file))
        out_file = os.path.join(self.config.output_reasoned_dir, file.replace('.n3', '.ttl'))
        run_reasoner(os.path.join(root, file), out_file, self.config.reasoner_config, self.config.ontopilot)

def main():
    parser = argparse.ArgumentParser(
        description="ontology data pipeline command line application.",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on "
               "the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')

    parser.add_argument(
        "data_file",
        help="Specify the data file to load."
    )
    parser.add_argument(
        "output_dir",
        help="path of the directory to place the processed data"
    )

    parser.add_argument(
        "ontology",
        help="specify a filepath/url of the ontology to use for reasoning/triplifying"
    )

    parser.add_argument(
        "config_dir",
        help="Specify the path of the directory containing the configuration files."
    )

    parser.add_argument(
        "--drop_invalid",
        help="Drop any data that does not pass validation, log the results, and continue the process",
        action="store_true"
    )
    parser.add_argument(
        "--log_file",
        help="log all output to a log.txt file in the output_dir. default is to log output to the console",
        action="store_true"
    )
    parser.add_argument(
        "--reasoner_config",
        help="optionally specify the reasoner configuration file. Default is to look for reasoner.config in the configuration directory"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="verbose logging output",
        action="store_true"
    )
    parser.add_argument(
        "-c",
        "--chunk_size",
        help="chunk size to use when processing data. optimal chunk_size for datasets with less then 200000 records"
             "can be determined with: num_records / num_cpus",
        type=int,
        default=50000
    )
    parser.add_argument(
        "--num_processes",
        help="number of process to use for parallel processing of data. Defaults to cpu_count of the machine",
        type=int,
        default=multiprocessing.cpu_count()
    )
    args = parser.parse_args()

    if args.verbose:
        print("configuring...")

    config = Config(**args.__dict__)

    process = Process(config)
    process.run()
