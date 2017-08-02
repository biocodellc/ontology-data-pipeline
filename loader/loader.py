# -*- coding: utf-8 -*-

import argparse
import os

from loader import blazegraph_loader
from loader.rdf2csv import RDF2CSV
from process.utils import clean_dir
from .es_loader import ESLoader

__version__ = "0.1.0"

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), '../config')
DEFAULT_SPARQL = os.path.join(DEFAULT_CONFIG, 'fetch_reasoned.sparql')

ELASTIC_SEARCH = "elasticsearch"
BLAZEGRAPH = "blazegraph"
BOTH = "both"

FILENAME_NAMESPACE = "filename"


def run(args):
    if args.action in [BLAZEGRAPH, BOTH]:
        blazegraph_loader.load(args.input_dir)

    if args.action in [ELASTIC_SEARCH, BOTH]:
        output_dir = args.reasoned_csv_output or os.path.join(args.input_dir, '../output_reasoned_csv')
        clean_dir(output_dir)

        rdf_converter = RDF2CSV(args.input_dir, output_dir, get_sparql(args.sparql_file or DEFAULT_SPARQL))
        rdf_converter.convert()

        ESLoader(output_dir, args.index, args.drop_existing, args.alias)


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

    parser.add_argument(
        "input_dir",
        help="The path of the directory containing the data to upload"
    )

    bg_group = parser.add_argument_group('blazegraph', 'blazegraph loading options')
    bg_group.add_argument('--sparql_file',
                          help='file containing the sparql query used to convert the input rdf files to csv format for '
                               'uploading to elasticsearch. defaults to ' + DEFAULT_SPARQL)
    bg_group.add_argument('--endpoint', default=blazegraph_loader.DEFAULT_ENDPOINT,
                          help='the blazegraph endpoint to upload to. The namespace will be the name of the uploaded'
                               'file minus the extension')

    es_group = parser.add_argument_group('elastic_search', 'elastic_search loading options')
    es_group.add_argument(
        "index",
        help="The name elasticsearch of the index to upload to"
    )
    es_group.add_argument('--reasoned_csv_output',
                          help='directory to place the generated csv files from the rdf input. defaults to '
                               'input_dir + ../output_reasoned_csv/')
    es_group.add_argument('--drop-existing', dest='drop_existing', action='store_true',
                          help='this flag will drop all existing data with the same "source" value.')
    es_group.add_argument('--alias',
                          help='optionally specify an elastic search alias. When creating an index, it will be associated'
                               'with this alias')
    args = parser.parse_args()

    if args.action in [BLAZEGRAPH, BOTH]:
        if not args.sparql_file:
            parser.error(
                'without --sparql_file, --sparql_file is required if action is {} or {}'.format(BLAZEGRAPH, BOTH))

    run(args)
