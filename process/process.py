# -*- coding: utf-8 -*-

import argparse
import os
import pandas as pd

from process.splitter import split_file
from process.utils import loadPreprocessorFromProject, loadClass, clean_dir
from .config import Config, DEFAULT_CONFIG_DIR
from .reasoner import run_reasoner
from .triplifier import Triplifier
from .validator import Validator

"""process.Process: provides entry point main()."""
__version__ = "0.1.0"

PROJECT_BASE = os.path.join(os.path.dirname(__file__), '../projects')


class Process(object):
    def __init__(self, config):
        self.config = config

        if not self.config.data_file:
            self.config.data_file = self.__preprocess_data()

        self.triplifier = Triplifier(config)

    def run(self):
        clean_dir(self.config.output_unreasoned_dir)
        clean_dir(self.config.output_reasoned_dir)

        if self.config.split_data_column:
            self.__split_and_triplify_data()
        else:
            self.__triplify_data()

        for root, dirs, files in os.walk(self.config.output_unreasoned_dir):
            c = 0
            for f in files:
                c += 1
                if self.config.verbose:
                    print("\trunning reasoner on {} of {} files".format(c, len(files)), file=self.config.log_file)

                out_file = os.path.join(self.config.output_reasoned_dir, f.replace('.n3', '.ttl'))
                run_reasoner(os.path.join(root, f), out_file, self.config.reasoner_config)

    def __split_and_triplify_data(self):
        if self.config.verbose:
            print("\tsplitting input data on {}".format(self.config.split_data_column), file=self.config.log_file)

        clean_dir(self.config.output_csv_split_dir)

        split_file(self.config.data_file, self.config.output_csv_split_dir, self.config.split_data_column,
                   self.config.chunk_size)

        for root, dirs, files in os.walk(self.config.output_csv_split_dir):
            for f in files:
                data = pd.read_csv(os.path.join(root, f), header=0, skipinitialspace=True)

                if self.config.verbose:
                    print("\tvalidating records in {}".format(f), file=self.config.log_file)

                triples_file = os.path.join(self.config.output_unreasoned_dir, f.replace('.csv', '.n3'))
                self.__triplify(data, triples_file)

    def __triplify_data(self):
        data = pd.read_csv(self.config.data_file, header=0, skipinitialspace=True, chunksize=self.config.chunk_size)

        i = 1
        for chunk in data:
            if self.config.verbose:
                print("\tvalidating {} records".format(len(chunk)), file=self.config.log_file)

            triples_file = os.path.join(self.config.output_unreasoned_dir, 'data_' + i)
            self.__triplify(chunk, triples_file)
            i += 1

    def __triplify(self, data, triples_file):

        validator = Validator(self.config, data)
        valid = validator.validate()

        if not valid:
            invalid_data_path = os.path.realpath(self.config.invalid_data_file.name)
            if self.config.log_file:
                log_path = os.path.realpath(self.config.log_file.name)
                print("Validation Failed! The logs are located {} and {}".format(invalid_data_path, log_path))
            else:
                print("Validation Failed! The logs are located {}".format(invalid_data_path))
            exit()

        if self.config.verbose:
            print("\ttriplifying {} records".format(len(data)), file=self.config.log_file)

        triples = self.triplifier.triplify(data)

        with open(triples_file, 'w') as f:
            for t in triples:
                f.write("{} .\n".format(t))

    def __preprocess_data(self):
        clean_dir(self.config.output_csv_dir)
        if self.config.preprocessor:
            PreProcessor = loadClass(self.config.preprocessor)
        else:
            PreProcessor = loadPreprocessorFromProject(self.config.base_dir)

        preprocessor = PreProcessor(self.config.input_dir, self.config.output_csv_dir)
        preprocessor.run()

        return preprocessor.output_file_path


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

    group.add_argument(
        "--config_dir",
        help="optionally specify the path of the directory containing the configuration files. defaults to " + DEFAULT_CONFIG_DIR
    )
    parser.add_argument(
        "--ontology",
        help="optionally specify a filepath/url of the ontology to use for reasoning/triplifying"
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
        help="chunk size to use when processing data. defaults to 50,000"
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

    config = Config(os.path.join(PROJECT_BASE, args.project), **args.__dict__)

    process = Process(config)
    process.run()
