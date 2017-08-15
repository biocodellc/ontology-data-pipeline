# -*- coding: utf-8 -*-

import argparse
import os

from loader import blazegraph_loader
from .es_loader import ESLoader

__version__ = "0.1.0"

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), '../config')

ELASTIC_SEARCH = "elasticsearch"
BLAZEGRAPH = "blazegraph"
BOTH = "both"

FILENAME_NAMESPACE = "filename"


def run(args):
    if args.action in [BLAZEGRAPH, BOTH]:
        blazegraph_loader.load(args.rdf_input_dir)

    if args.action in [ELASTIC_SEARCH, BOTH]:
        ESLoader(args.es_input_dir, args.index, args.drop_existing, args.alias)


def get_sparql(file):
    with open(file) as f:
        return f.read()


def main():
    parser = argparse.ArgumentParser(
        description="data loading cmd line application for PPO data pipeline.",
        epilog="As an alternative to the commandline, params can be placed in a file, one per line, and specified on "
               "the commandline like '%(prog)s @params.conf'.",
        fromfile_prefix_chars='@')

    parser.add_argument('action', choices={ELASTIC_SEARCH, BLAZEGRAPH, BOTH}, default=BOTH)

    bg_group = parser.add_argument_group('blazegraph', 'blazegraph loading options')
    bg_group.add_argument(
        "--rdf_input_dir",
        help="The path of the directory containing the rdf data to upload to blazegraph"
    )
    bg_group.add_argument('--endpoint', default=blazegraph_loader.DEFAULT_ENDPOINT,
                          help='the blazegraph endpoint to upload to. The namespace will be the name of the uploaded'
                               'file minus the extension')

    es_group = parser.add_argument_group('elastic_search', 'elastic_search loading options')
    es_group.add_argument(
        "--es_input_dir",
        help="The path of the directory containing the csv data to upload to elasticsearch"
    )
    es_group.add_argument(
        "--index",
        help="The name elasticsearch of the index to upload to"
    )
    es_group.add_argument('--drop-existing', dest='drop_existing', action='store_true',
                          help='this flag will drop all existing data with the same "source" value.')
    es_group.add_argument('--alias',
                          help='optionally specify an elastic search alias. When creating an index, it will be associated'
                               'with this alias')
    args = parser.parse_args()

    if args.action in [BLAZEGRAPH, BOTH]:
        if not args.rdf_input_dir:
            parser.error(
                'without --rdf_input_dir, --rdf_input_dir is required if action is {} or {}'.format(BLAZEGRAPH, BOTH))
    if args.action in [ELASTIC_SEARCH, BOTH]:
        if not args.es_input_dir:
            parser.error(
                'without --es_input_dir, --es_input_dir is required if action is {} or {}'.format(BLAZEGRAPH, BOTH))
        if not args.index:
            parser.error(
                'without --index, --index is required if action is {} or {}'.format(BLAZEGRAPH, BOTH))

    run(args)
