# -*- coding: utf-8 -*-
import csv
import os
import rdflib


def convert_rdf2csv(file, output_dir, sparql):
    g = rdflib.Graph()
    g.parse(file, format="turtle")

    out_file = os.path.join(output_dir, file.rsplit('/')[-1].replace('.ttl', '.csv'))
    with open(out_file, 'w') as f:
        writer = csv.writer(f)
        results = g.query(sparql)

        writer.writerow(results.vars)  # header
        for row in results:
            writer.writerow(row)
