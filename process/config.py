# -*- coding: utf-8 -*-

import os
import csv
import re
import rfc3987
from .labelmap import LabelMap

VALID_RULES = ['RequiredValue', 'ControlledVocabulary', 'UniqueValue', 'Integer', 'Float']
DEFAULT_ONTOLOGY = "https://github.com/PlantPhenoOntology/PPO/raw/master/ontology/ppo-reasoned.owl"
DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../reasoner.conf")

DEFAULT_HEADERS = ['record_id', 'scientific_name', 'genus', 'specific_epithet', 'year', 'day_of_year', 'latitude',
                   'longitude', 'source', 'phenophase_name', 'lower_count', 'upper_count', 'lower_percent',
                   'upper_percent']


class Config(object):
    """
    class containg config values. All config data is accessible as attributes on this class
    """

    def __init__(self, base_dir, *initial_data, **kwargs):
        """
        :param base_dir: path to the project directory containing the project files
        :param initial_data: dict arguments used to extend the Config class
        :param kwargs: kwargs used to extend the Config class
        """
        for dict in initial_data:
            for key in dict:
                setattr(self, key, dict[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

        if self.log_file:
            self.log_file = open(os.path.join(self.output_dir, 'log.txt'), 'w')
        else:
            self.log_file = None

        self.invalid_data_file = open(os.path.join(self.output_dir, 'invalid_data.csv'), 'w')
        self.base_dir = base_dir
        if not self.config_dir:
            self.config_dir = DEFAULT_CONFIG_DIR

        if not self.reasoner_config:
            self.reasoner_config = os.path.join(self.config_dir, "reasoner.conf")

        if not self.ontology:
            self.ontology = DEFAULT_ONTOLOGY
        self.__label_map = LabelMap(self.ontology)

        self.__parse_headers()

        self.lists = {}
        self.rules = []
        self.__parse_rules()
        self.__add_default_rules()

        self.entities = []
        self.__parse_entities()
        self.__parse_mapping()
        self.relations = []
        self.__parse_relations()

    def __getattr__(self, item):
        """
        fallback if attribute isn't found
        """
        return None

    def get_entity(self, alias):
        """
        Return the entity with a matching alias
        """

        for entity in self.entities:
            if entity['alias'] == alias:
                return entity

    def get_list(self, column):
        """
        Return the list for a given column
        """

        for rule in self.rules:
            if rule['rule'] == 'ControlledVocabulary' and column in rule['columns']:
                return self.lists[rule['list']]

    def __parse_rules(self):
        """
        Parse rules.csv file for the project. Used to define data validation rules.
        Expected columns are: rule,columns,level,list
        """
        file = os.path.join(self.config_dir, 'rules.csv')

        if not os.path.exists(file):
            return

        with open(file) as f:
            reader = csv.DictReader(f)
            self.rules = [d for d in reader]

        for rule in self.rules:
            if rule['rule'] not in VALID_RULES:
                raise AttributeError("Invalid rule in \"{}\". {} is not a valid rule [{}]".format(file, rule['rule'],
                                                                                                  ",".join(
                                                                                                      VALID_RULES)))
            if rule['rule'] == 'ControlledVocabulary':
                if not rule['list']:
                    raise AttributeError(
                        "Invalid rule in \"{}\". ControlledVocabulary rule must specify a list".format(file))

                self.__parse_list(rule['list'])

            if not rule['columns']:
                raise AttributeError("Invalid rule in \"{}\". All rules must specify columns.".format(file))
            else:
                rule['columns'] = [c.strip() for c in rule['columns'].split('|')]

            if not rule['level']:
                rule['level'] = 'warning'

    def __parse_list(self, file_name):
        """
        Parse list_name.csv file. The file name is specified in the list column of the rules.csv file and contains the
        controlled vocabulary, 1 field per line.
        """
        if file_name not in self.lists:
            file_path = os.path.join(self.base_dir, file_name)

            if not os.path.exists(file_path):
                raise AttributeError(
                    "Invalid rule. Can't find specified list \"{}\"".format(file_path))

            with open(file_path) as file:
                reader = csv.DictReader(file)
                self.lists[file_name] = []

                for r in reader:
                    if r['defined_by']:
                        r['defined_by'] = self.__get_uri_from_label(r['defined_by'])
                        self.lists[file_name].append(r)

    def __add_default_rules(self):
        list_name = 'phenophase_descriptions.csv'
        self.rules.append({
            'rule': 'ControlledVocabulary',
            'columns': ['phenophase_name'],
            'level': 'error',
            'list': list_name
        })

        self.__parse_list(list_name)

    def __parse_entities(self):
        """
        Parse entity.csv file for the project. Used to define the entities for triplifying
        Expected columns are: alias,concept_uri,unique_key,identifier_root
        """
        file = os.path.join(self.config_dir, 'entity.csv')

        if not os.path.exists(file):
            raise RuntimeError("entity.csv file missing from project configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            self.entities = [d for d in reader]

        for entity in self.entities:
            if not entity['alias'] or not entity['concept_uri'] or not entity['unique_key']:
                raise RuntimeError(
                    "Invalid entity in {}. alias, concept_uri, and unique_key are required for each entity"
                    "listed.".format(file))

            entity['concept_uri'] = self.__get_uri_from_label(entity['concept_uri'])
            entity['columns'] = []

    def __parse_mapping(self):
        """
        Parse mapping.csv file for the project, and add the columns to the specified entities. Used to define the
        mapping of the data csv to entities for triplifying.
        Expected columns are: column,entity_alias
        """
        file = os.path.join(self.config_dir, 'mapping.csv')

        if not os.path.exists(file):
            raise RuntimeError("mapping.csv file missing from project configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)

            for mapping in reader:
                entity = self.get_entity(mapping['entity_alias'])

                if not entity:
                    raise RuntimeError('Invalid entity_alias "{}" defined in {}'.format(mapping['entity_alias'], file))

                entity['columns'].append((mapping['column'], self.__get_uri_from_label(mapping['uri'])))

    def __parse_relations(self):
        """
        Parse relations.csv file for the project. Used to define the relations between entities for triplifying
        Expected columns are: subject_entity_alias,predicate,object_entity_alias
        """
        file = os.path.join(self.config_dir, 'relations.csv')

        if not os.path.exists(file):
            raise RuntimeError("relations.csv file missing from project configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            self.relations = [d for d in reader]

        for r in self.relations:
            if not r['subject_entity_alias'] or not r['predicate'] or not r['object_entity_alias']:
                raise RuntimeError(
                    "Invalid relation in {}. subject_entity_alias, predicate, and object_entity_alias are required for "
                    "each relation listed.".format(file))

            r['predicate'] = self.__get_uri_from_label(r['predicate'])

    def __get_uri_from_label(self, def_text):
        """
        Fetches a URI given a label by searching
        all term labels in braces ('{' and '}').  For
        example, if we encounter "{whole plant phenological stage}",
        it will be converted to "http://purl.obolibrary.org/obo/PPO_0000001".
        """
        labelre = re.compile(r'(\{[A-Za-z0-9\- _]+\})')
        defparts = labelre.split(def_text)

        newdef = ''
        for defpart in defparts:
            if labelre.match(defpart):
                label = defpart.strip("{}")

                # Get the class IRI associated with this label.
                try:
                    labelIRI = self.__label_map.lookupIRI(label)
                except KeyError:
                    raise RuntimeError('The class label, "' + label
                                       + '", could not be matched to a term IRI.')

                newdef = str(labelIRI)
            else:
                newdef += defpart

        if len(defparts) == 0:
            newdef = def_text

        if len(newdef) != 0:
            # attempt parsing wlth the rfc3987 library and throws error if not a valid IRI
            rfc3987.parse(newdef, rule='IRI')

        return newdef

    def __parse_headers(self):
        file = os.path.join(self.config_dir, 'headers.csv')

        if not os.path.exists(file):
            self.headers = DEFAULT_HEADERS
            return

        with open(file) as f:
            reader = csv.reader(f)
            self.headers = reader[0]
