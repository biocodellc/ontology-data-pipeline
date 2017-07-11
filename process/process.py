# -*- coding: utf-8 -*-


"""process.Process: provides entry point main()."""
__version__ = "0.1.0"

import argparse, importlib


def run(args):
    module_name, class_name = args.preprocessor.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    PreProcessor = getattr(mod, class_name)

    preprocessor = PreProcessor(args.input_dir, args.output_dir)

    preprocessor.run()
    print(preprocessor.output_file_path)


def main():
    parser = argparse.ArgumentParser(
        description="PPO data pipeline cmd line application.",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on "
               "the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')
    parser.add_argument(
        "preprocessor",
        help="the dotted python path of the preprocessor class"
    )
    parser.add_argument(
        "input_dir",
        help="path of the directory containing the data to process"
    )
    parser.add_argument(
        "output_dir",
        help="path of the directory to place the processed data"
    )
    args = parser.parse_args()

    run(args)
