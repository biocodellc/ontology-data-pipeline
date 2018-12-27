# -*- coding: utf-8 -*-

import argparse
import logging
import os
import math

import multiprocessing
from itertools import repeat

import pandas as pd

from .rdf2csv import convert_rdf2csv
from .splitter import split_file
from .utils import  loadClass, clean_dir, fetch_ontopilot, fetch_query_fetcher
from .config import Config
from .reasoner import run_reasoner
from .triplifier import Triplifier
from .validator import Validator

"""process.Process: provides entry point main()."""
__version__ = "0.1.0"



class Process(object):
    def __init__(self, config):
        self.config = config

        if not self.config.data_file:
            self.config.data_file = self._preprocess_data()

        self.triplifier = Triplifier(config)
        self.validator = Validator(config)

    def run(self):
        print('step1')
        fetch_ontopilot(self.config.ontopilot, self.config.ontopilot_repo_url)
        fetch_query_fetcher(self.config.queryfetcher, self.config.queryfetcher_repo_url)
        print('step2')

        clean_dir(self.config.output_unreasoned_dir)
        clean_dir(self.config.output_reasoned_dir)
        print('step3')
        if self.config.reasoned_sparql:
            clean_dir(self.config.output_reasoned_csv_dir)

        if self.config.split_data_column:
            self._split_and_triplify_data()
        else:
            self._triplify_data()

        print('step4')
        self._reason_all()

        print('step5')
        if self.config.reasoned_sparql:
            self._reasoned2csv()

    def _reason_all(self):
        num_processes = math.floor(self.config.num_processes / 2)
        if num_processes < 1:
            num_processes = 1

        for root, dirs, files in os.walk(self.config.output_unreasoned_dir):
            with multiprocessing.Pool(processes=num_processes) as pool:
                pool.starmap(self._reason, zip(files, repeat(root)))

    def _csv2rdf(self, file):
        logging.debug("\trunning csv2reasoner on {}".format(file))
        out_file = os.path.join(self.config.output_reasoned_dir, file.replace('.n3', '.ttl'))
        convert_rdf2csv(os.path.join(self.config.output_reasoned_dir,file),self.config.output_reasoned_csv_dir,
                        self.config.reasoned_sparql, self.config.queryfetcher)

    def _reasoned2csv(self):
        num_processes = math.floor(self.config.num_processes / 2)
        files = []
        for file in os.listdir(self.config.output_reasoned_dir):
            if file.endswith(".ttl"):
                files.append(file)

        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(self._csv2rdf, zip(files))

    def _split_and_triplify_data(self):
        """
        splits the parent dataframe into many files based on the config.split_data_column. There will be a minimum of
        1 file for each unique value in the split_data_column column, with config.chunk_size being the max number of
        records in each file
        """
        logging.debug("\tsplitting input data on {}".format(self.config.split_data_column))

        clean_dir(self.config.output_csv_split_dir)

        split_file(self.config.data_file, self.config.output_csv_split_dir, self.config.split_data_column,
                   self.config.chunk_size)

        for root, dirs, files in os.walk(self.config.output_csv_split_dir):
            files = [os.path.join(root, f) for f in files]

            with multiprocessing.Pool(processes=self.config.num_processes) as pool:
                pool.map(self._triplify_file, files)

    def _triplify_file(self, file):
        data = pd.read_csv(file, header=0, skipinitialspace=True)

        logging.debug("\tvalidating records in {}".format(file))

        triples_file = os.path.join(self.config.output_unreasoned_dir, file.rsplit('/', 1)[-1].replace('.csv', '.n3'))
        self._triplify(data, triples_file)

    def _triplify_data(self):
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

    def _preprocess_data(self):

        clean_dir(self.config.output_csv_dir)
        if self.config.preprocessor:
            PreProcessor = loadClass(self.config.preprocessor, "PreProcessor")
        else:
            PreProcessor = loadClass(os.path.join(self.config.project_base, self.config.project, "preprocessor.py"), "PreProcessor")

        preprocessor = PreProcessor(self.config.input_dir, self.config.output_csv_dir)
        preprocessor.run()

        return preprocessor.output_file


def main():
    parser = argparse.ArgumentParser(
        description="PPO data pipeline cmd line application.",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on "
               "the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')
    parser.add_argument(
        "project",
        help="This is the name of the directory containing the project specific files. All project config directories" +
             "must be placed in the `projects` directory."
    )
    parser.add_argument(
        "output_dir",
        help="path of the directory to place the processed data"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--input_dir",
        help="path of the directory containing the data to process"
    )
    group.add_argument(
        "--data_file",
        help="optionally specify the data file to load. This will skip the preprocessor step and used the supplied data "
             "file instead",
        dest="data_file"
    )

    parser.add_argument(
        "--ontology",
        help="specify a filepath/url of the ontology to use for reasoning/triplifying",
        required=True
    )
    parser.add_argument(
        "--headers",
        help="specify a filepath/url of the headers to use for input and output",
        required=True
    )

    parser.add_argument(
        "--config_dir",
        help="Specify the path of the directory containing the configuration files.",
        required=True
    )
    parser.add_argument(
        "--project_base",
        help="Specify where the python modules reside for the preprocessor live.  This is specified in python dotted notation. specifying project_base you will likely want to specify a custom base_dir as well.",
        required=True
    )
    parser.add_argument(
        "--base_dir",
        help="Specify the the base directory containing the project files.",
        required=True
    )
    parser.add_argument(
        "--preprocessor",
        help="optionally specify the dotted python path of the preprocessor class. This will be loaded instead of "
             "looking for a PreProcessor in the supplied project directory. \n Ex:\t projects.asu.proprocessor.PreProcessor",
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
        help="optionally specify the reasoner configuration file"
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
    parser.add_argument(
        "-s",
        "--split_data",
        help="column to split the data on. This will split the data file into many files with each file containing no "
             "more records then the specified chunk_size, using the specified column values as the filenames",
        dest="split_data_column"
    )
    args = parser.parse_args()

    if args.verbose:
        print("configuring...")

    #config = Config(os.path.join(DEFAULT_BASE_DIR, args.project), **args.__dict__)
    config = Config(**args.__dict__)

    process = Process(config)
    process.run()
