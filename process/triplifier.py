# -*- coding: utf-8 -*-
import re
import pandas as pd
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import logging
from .utils import isNull


class Triplifier(object):
    def __init__(self, config):
        self.config = config
        self.integer_columns = []
        for rule in self.config.rules:
            if rule['rule'].lower() == 'integer':
                self.integer_columns.extend(rule['columns'])

    def triplify(self, data_frame):
        """
        Generate triples using the given data_frame and the config mappings

        :param data_frame: pandas DataFrame
        :return: list of triples for the given data_frame data
        """
        triples = []

        data_frame = data_frame.fillna('')

        for index, row in data_frame.iterrows():
            triples.extend(self._generate_triples_for_row(row))

        triples.extend(self._generate_triples_for_relation_predicates())
        triples.extend(self._generate_triples_for_entities())
        triples.append(self._generate_ontology_import_triple())

        return triples

    def _generate_triples_for_chunk(self, chunk):
        triples = []
        for index, row in chunk.iterrows():
            triples.extend(self._generate_triples_for_row(row))

        return triples

    def _generate_triples_for_row(self, row):
        row_triples = []

        for entity in self.config.entities:
            s = "<{}{}>".format(entity['identifier_root'], self._get_value(row, entity['unique_key']))
            if entity['concept_uri'] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                o = "<{}>".format(entity['concept_uri'])
                row_triples.append("{} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {}".format(s, o))

            for column, uri in entity['columns']:
                val = self._get_value(row, column)

                list_for_column = self.config.get_list(column)

                # if there is a specified list for this column & the field contains a defined_by, substitute the
                # defined_by value for the list field
                literal_val = True
                if list_for_column and "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" in uri:
                    for i in list_for_column:
                        if i['field'] == val and i['defined_by']:
                            val = i['defined_by']
                            literal_val = False
                            break

                # format and print all of the instance data triples
                if (not isNull(val)):
                    p = "<{}>".format(uri)
                    if literal_val:
                        type = self._get_type(val)
                        o = "\"{}\"^^<http://www.w3.org/2001/XMLSchema#{}>".format(val, type)
                    else:
                        o = "<{}>".format(str(val))
                    row_triples.append("{} {} {}".format(s, p, o))

        # format and print all triples describing relations
        for relation in self.config.relations:
            try:
                subject_entity = self.config.get_entity(relation['subject_entity_alias'])
                object_entity = self.config.get_entity(relation['object_entity_alias'])
            
                s = "<{}{}>".format(subject_entity['identifier_root'], self._get_value(row, subject_entity['unique_key']))
                p = "<{}>".format(relation['predicate'])
                o = "<{}{}>".format(object_entity['identifier_root'], self._get_value(row, object_entity['unique_key']))
                row_triples.append("{} {} {}".format(s, p, o))
            except Exception as err:
                raise RuntimeError("Error assigning relations between a subject and an object.  "
                "Check to be sure each relation maps to an entity alias")

        return row_triples

    def _generate_triples_for_relation_predicates(self):
        predicate_triples = []

        for relation in self.config.relations:
            s = "<{}>".format(relation['predicate'])
            p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            o = "<http://www.w3.org/2002/07/owl#ObjectProperty>"
            predicate_triples.append("{} {} {}".format(s, p, o))

        return predicate_triples

    def _generate_triples_for_entities(self):
        entity_triples = []

        for entity in self.config.entities:
            entity_triples.extend(self._generate_property_triples(entity['columns']))
            if entity['concept_uri'] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                entity_triples.append(self._generate_class_triple(entity['concept_uri']))

        return entity_triples

    def _generate_ontology_import_triple(self):
        s = "<urn:importInstance>"
        p = "<http://www.w3.org/2002/07/owl#imports>"
        o = "<{}>".format(self.config.ontology)

        return "{} {} {}".format(s, p, o)

    @staticmethod
    def _generate_class_triple(concept_uri):
        s = "<{}>".format(concept_uri)
        p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        o = "<http://www.w3.org/2000/01/rdf-schema#Class>"

        return "{} {} {}".format(s, p, o)

    @staticmethod
    def _generate_property_triples(properties):
        """
        generate triples for the properties of each entity
        """
        property_triples = []

        for column, uri in properties:
            s = "<{}>".format(uri)
            p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            o = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>"
            property_triples.append("{} {} {}".format(s, p, o))

            o2 = "<http://www.w3.org/2002/07/owl#DatatypeProperty>"
            property_triples.append("{} {} {}".format(s, p, o2))

            p2 = "<http://www.w3.org/2000/01/rdf-schema#isDefinedBy>"
            property_triples.append("{} {} {}".format(s, p2, s))

        return property_triples

    def _get_value(self, row_data, column):
        coerce_integer = False

        if column in self.integer_columns:
            coerce_integer = True

        val = str(row_data[column])

        # need to perform coercion here as pandas can't store ints along floats and strings. The only way to coerce
        # to ints is to drop all strings and null values. We don't want to do this in the case of a warning.
        if coerce_integer:
            return int(float(val)) if re.fullmatch("[+-]?\d+(\.0+)?", str(val)) else val

        return val

    @staticmethod
    def _get_type(val):
        if re.fullmatch("[+-]?\d+", str(val)):
            return 'integer'
        elif re.fullmatch("[+-]?\d+\.\d+", str(val)):
            return 'float'
        else:
            return 'string'
