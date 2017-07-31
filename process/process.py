# -*- coding: utf-8 -*-

import argparse
import importlib
import os
import pandas as pd
import tempfile

import shutil

from .config import Config
from .reasoner import run_reasoner
from .triplifier import Triplifier
from .validator import Validator

"""process.Process: provides entry point main()."""
__version__ = "0.1.0"

PROJECT_BASE = os.path.join(os.path.dirname(__file__), '../projects')


def run(config):
    if not config.data_file:
        config.data_file = __preprocess_data(config)

    data = pd.read_csv(config.data_file, header=0, skipinitialspace=True, chunksize=100000)

    triplifier = Triplifier(config)

    tmp_triples_dir = tempfile.mkdtemp()
    tmp_reasoned_dir = tempfile.mkdtemp()
    try:
        for chunk in data:
            if config.verbose:
                print("\tvalidating {} records".format(len(chunk)), file=config.log_file)

            validator = Validator(config, chunk)
            valid = validator.validate()

            if not valid:
                invalid_data_path = os.path.realpath(config.invalid_data_file.name)
                if config.log_file:
                    log_path = os.path.realpath(config.log_file.name)
                    print("Validation Failed! The logs are located {} and {}".format(invalid_data_path, log_path))
                else:
                    print("Validation Failed! The logs are located {}".format(invalid_data_path))
                exit()

            if config.verbose:
                print("\ttriplifying {} records".format(len(chunk)), file=config.log_file)

            triples = triplifier.triplify(chunk)

            with tempfile.NamedTemporaryFile(mode="w", dir=tmp_triples_dir, delete=False) as tmp:
                for t in triples:
                    tmp.write("{} .\n".format(t))

        for root, dirs, files in os.walk(tmp_triples_dir):
            c = 0
            for f in files:
                c += 1
                if config.verbose:
                    print("\trunning reasoner on {} of {} files".format(c, len(files)), file=config.log_file)

                out_file = tempfile.NamedTemporaryFile(dir=tmp_reasoned_dir, delete=False).name
                run_reasoner(os.path.join(root, f), out_file, config.reasoner_config)
                print (out_file)

    finally:
        if config.verbose:
            print("\tremoving temporary files", file=config.log_file)
        shutil.rmtree(tmp_triples_dir)
        shutil.rmtree(tmp_reasoned_dir)


def __preprocess_data(config):
    if config.preprocessor:
        PreProcessor = __loadClass(config.preprocessor)
    else:
        PreProcessor = __loadPreprocessorFromProject(config.base_dir)

    preprocessor = PreProcessor(config.input_dir, config.output_dir)
    preprocessor.run()

    return preprocessor.output_file_path


def __loadClass(python_path):
    """
    dynamically loads a class given a dotted python path
    :param python_path:
    :return:
    """
    module_name, class_name = python_path.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)


def __loadPreprocessorFromProject(base_dir):
    """
    loads a PreProcessor from the specified base_dir

    Assumptions:
        1. base_dir contains a file `preprocessor.py`
        2. in this file, there is a class named *PreProcessor

    :param base_dir:
    :return:
    """
    path = os.path.join(base_dir, 'preprocessor.py')
    if not os.path.exists(path):
        raise ImportError("Error loading preprocessor. File does not exist `{}`.".format(path))

    spec = importlib.util.spec_from_file_location(path, path)
    module = spec.loader.load_module()
    try:
        return getattr(module, 'PreProcessor')
    except AttributeError:
        raise ImportError(
            "Error loading preprocessor. Could not find a PreProcessor class in the file `{}`".format(path))


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
    args = parser.parse_args()

    if args.verbose:
        print("configuring...")

    config = Config(os.path.join(PROJECT_BASE, args.project), **args.__dict__)
    run(config)
