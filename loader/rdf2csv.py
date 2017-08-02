# -*- coding: utf-8 -*-
import rdflib

from loader.utils import get_files


class RDF2CSV(object):
    def __init__(self, input_dir, output_dir, sparql_query):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.sparql_query = sparql_query

    def convert(self):
        for file in get_files(self.input_dir, '.ttl'):
            g = rdflib.Graph()
            g.parse(file, format="turtle")

            for row in g.query(self.sparql_query):
                print(row)
                #TODO finish this
