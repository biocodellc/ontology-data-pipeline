# -*- coding: utf-8 -*-


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

        for row in data_frame:
            triples.extend(self.__generate_triples_for_row(row))

        return triples

    def __generate_triples_for_row(self, row):
        row_triples = []

        for entity in self.config.entities:
            s = "<{}{}>".format(entity['identifier'], row[entity['unique_key']])
            o = "<{}>".format(entity['concept_uri'])
            row_triples.append("{} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {}".format(s, o))

            for column, uri in entity['columns']:
                p = "<{}>".format(uri)
                o = "\"{}\"".format(row[column])
                row_triples.append("{} {} {}".format(s, p, o))

        for relation in self.config.relations:
            subject_entity = self.config.get_entity(relation['subject_entity_alias'])
            object_entity = self.config.get_entity(relation['object_entity_alias'])
            s = "<{}{}>".format(subject_entity['identifier'], row[subject_entity['unique_key']])
            p = "<{}>".format(relation['predicate'])
            o = "<{}{}>".format(object_entity['identifier'], row[object_entity['unique_key']])
            row_triples.append("{} {} {}".format(s, p, o))

        return row_triples
