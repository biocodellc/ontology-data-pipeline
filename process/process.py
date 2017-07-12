# -*- coding: utf-8 -*-

import argparse
import importlib
import os

from process.config import Config

"""process.Process: provides entry point main()."""
__version__ = "0.1.0"

PROJECT_BASE = os.path.join(os.path.dirname(__file__), '../projects')


def run(config):
    if config.preprocessor:
        PreProcessor = __loadClass(config.preprocessor)
    else:
        PreProcessor = __loadPreprocessorFromProject(config.base_dir)

    preprocessor = PreProcessor(config.input_dir, config.output_dir)

    preprocessor.run()
    print(preprocessor.output_file_path)


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
        "input_dir",
        help="path of the directory containing the data to process"
    )
    parser.add_argument(
        "output_dir",
        help="path of the directory to place the processed data"
    )
    parser.add_argument(
        "--preprocessor",
        help="optionally specify the dotted python path of the preprocessor class. This will be loaded instead of "
             "looking for a PreProcessor in the supplied project directory. \n Ex:\t projects.asu.proprocessor.PreProcessor",
        dest="preprocessor"
    )
    parser.add_argument(
        "--drop_invalid",
        help="Drop any data that does not pass validation, log the results, and continue the process",
        dest="drop_invalid",
        action="store_true"
    )
    parser.add_argument(
        "--log_file",
        help="log all output to a log.txt file in the output_dir. default is to log output to the console",
        dest="log_file",
        action="store_true"
    )
    args = parser.parse_args()

    config = Config(os.path.join(PROJECT_BASE, args.project), args)
    run(config)
