# -*- coding: utf-8 -*-


"""process.Process: provides entry point main()."""
__version__ = "0.1.0"

import argparse


def run():
    pass


def main():
    parser = argparse.ArgumentParser(
        description="PPO data pipeline cmd line application.",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')
    # parser.add_argument(
    #     "rest_service",
    #     help="location of the FIMS REST services",
    #     default="http://biscicol.org/biocode-fims/rest/v2/"
    # )
    # parser.add_argument(
    #     "project_id",
    #     help="project_id to validate/upload against",
    # )
    # parser.add_argument(
    #     "-u",
    #     "--upload",
    #     help="upload the dataset",
    #     dest="upload",
    #     action="store_true")
    # parser.add_argument(
    #     "--public",
    #     help="set the expedition to public. defaults to false",
    #     dest="is_public",
    #     action="store_true")
    args = parser.parse_args()

    # if args.upload:
    #     if not args.expedition_code:
    #         parser.error('expedition_code is required when uploading')
    #     if (not args.username) or (not args.password):
    #         parser.error('username and password are required when uploading')

    run()
