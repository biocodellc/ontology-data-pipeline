# -*- coding: utf-8 -*-
import pandas as pd


class Triplifier(object):
    def __init__(self, config):
        self.config = config

    def triplify(self, data_frame):
        """
        Generate triples using the given data_frame and the config mappings

        :param data_frame: pandas DataFrame
        :return: list of triples for the given data_frame data
        """
        triples = []

        for index, row in data_frame.iterrows():
            triples.extend(self.__generate_triples_for_row(row))

        triples.extend(self.__generate_triples_for_relation_predicates())
        triples.extend(self.__generate_triples_for_entities())
        triples.append(self.__generate_ontology_import_triple())

        return triples

    def __generate_triples_for_row(self, row):
        row_triples = []

        for entity in self.config.entities:
            s = "<{}{}>".format(entity['identifier_root'], row[entity['unique_key']])
            if entity['concept_uri'] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                o = "<{}>".format(entity['concept_uri'])
                row_triples.append("{} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {}".format(s, o))

            for column, uri in entity['columns']:
                val = row[column]

                list_for_column = self.config.get_list(column)

                # if there is a specified list for this column & the field contains a defined_by, substitute the
                # defined_by value for the list field
                literal_val = True
                if list_for_column:
                    for i in list_for_column:
                        if i['field'] == val and i['defined_by']:
                            val = i['defined_by']
                            literal_val = False
                            break

                if val and not pd.isnull(val):
                    p = "<{}>".format(uri)
                    if literal_val:
                        o = "\"{}\"".format(val)
                    else:
                        o = "<{}>".format(val)
                    row_triples.append("{} {} {}".format(s, p, o))

        for relation in self.config.relations:
            subject_entity = self.config.get_entity(relation['subject_entity_alias'])
            object_entity = self.config.get_entity(relation['object_entity_alias'])
            s = "<{}{}>".format(subject_entity['identifier_root'], row[subject_entity['unique_key']])
            p = "<{}>".format(relation['predicate'])
            o = "<{}{}>".format(object_entity['identifier_root'], row[object_entity['unique_key']])
            row_triples.append("{} {} {}".format(s, p, o))

        return row_triples

    def __generate_triples_for_relation_predicates(self):
        predicate_triples = []

        for relation in self.config.relations:
            s = "<{}>".format(relation['predicate'])
            p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            o = "<http://www.w3.org/2002/07/owl#ObjectProperty>"
            predicate_triples.append("{} {} {}".format(s, p, o))

        return predicate_triples

    def __generate_triples_for_entities(self):
        entity_triples = []

        for entity in self.config.entities:
            entity_triples.extend(self.__generate_property_triples(entity['columns']))
            if entity['concept_uri'] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                entity_triples.append(self.__generate_class_triple(entity['concept_uri']))

        return entity_triples

    def __generate_ontology_import_triple(self):
        s = "<urn:importInstance>"
        p = "<http://www.w3.org/2002/07/owl#imports>"
        o = "<{}>".format(self.config.ontology)

        return "{} {} {}".format(s, p, o)

    @staticmethod
    def __generate_class_triple(concept_uri):
        s = "<{}>".format(concept_uri)
        p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        o = "<http://www.w3.org/2000/01/rdf-schema#Class>"

        return "{} {} {}".format(s, p, o)

    @staticmethod
    def __generate_property_triples(properties):
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
