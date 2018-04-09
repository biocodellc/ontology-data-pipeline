# -*- coding: utf-8 -*-
import logging
import os
import csv
import re
import rfc3987

from .labelmap import LabelMap

ONTOPILOT_VERSION = '2017-08-04'
ONTOPILOT_REPO_URL = 'http://repo.biocodellc.com/repository/3rd-party/org/biocode/ontopilot/{}/'.format(
    ONTOPILOT_VERSION)
QUERY_FETCHER_VERSION = '0.0.1'
QUERY_FETCHER_REPO_URL = 'http://repo.biocodellc.com/repository/maven-public/org/biocode/query_fetcher/{}/'.format(
    QUERY_FETCHER_VERSION)

DEFAULT_PROJECT_BASE = "projects"
DEFAULT_BASE_DIR = os.path.join(os.path.dirname(__file__), '../projects')

VALID_RULES = ['RequiredValue', 'ControlledVocabulary', 'UniqueValue', 'Integer', 'Float']
DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../config")
# NOTE: the default ontology used here is located at github.  Be wary of too many connections
# to this file at git...may want to specify a local filepath for the ontology instead
DEFAULT_ONTOLOGY = "https://raw.githubusercontent.com/PlantPhenoOntology/ppo/master/releases/2017-10-20/ppo.owl"

DEFAULT_HEADERS = ['record_id', 'scientific_name', 'genus', 'specific_epithet', 'year', 'day_of_year', 'latitude',
                   'longitude', 'source', 'phenophase_name', 'lower_count', 'upper_count', 'lower_percent',
                   'upper_percent']


class Config(object):
    """
    class containing config values. All config data is accessible as attributes on this class
    """

    def __init__(self, *initial_data, **kwargs):
        """
        :param initial_data: dict arguments used to extend the Config class
        :param kwargs: kwargs used to extend the Config class
        """
        for dict in initial_data:
            for key in dict:
                setattr(self, key, dict[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

        if not self.config_dir:
            self.config_dir = DEFAULT_CONFIG_DIR
        if not self.project_base:
            self.project_base = DEFAULT_PROJECT_BASE
        if not self.base_dir:
            self.base_dir = os.path.join(DEFAULT_BASE_DIR, self.project)
        if not self.ontology:
            self.ontology = DEFAULT_ONTOLOGY
        if not self.headers:
            self.headers = DEFAULT_HEADERS
        if not self.chunk_size:
            self.chunk_size = 50000

        
        self.ontopilot = os.path.join(os.path.dirname(__file__), '../lib/ontopilot-{}.jar'.format(ONTOPILOT_VERSION))
        self.ontopilot_repo_url = ONTOPILOT_REPO_URL
        self.queryfetcher = os.path.join(os.path.dirname(__file__), '../lib/query_fetcher-{}.jar'.format(QUERY_FETCHER_VERSION))
        self.queryfetcher_repo_url = QUERY_FETCHER_REPO_URL


        if self.log_file:
            logging.basicConfig(filename=os.path.join(self.output_dir, 'log.txt'), filemode='w')

        if self.verbose:
            logging.basicConfig(level=logging.DEBUG)

        data_file_path = os.path.join(self.output_dir, 'invalid_data.csv')
        if os.path.exists(data_file_path):
            os.remove(data_file_path)

        self.invalid_data_file = os.path.join(self.output_dir, 'invalid_data.csv')

        # output directories
        self.output_csv_dir = os.path.join(self.output_dir, 'output_csv')
        self.output_csv_split_dir = os.path.join(self.output_dir, 'output_csv_split')
        self.output_unreasoned_dir = os.path.join(self.output_dir, 'output_unreasoned')
        self.output_reasoned_dir = os.path.join(self.output_dir, 'output_reasoned')
        self.output_reasoned_csv_dir = os.path.join(self.output_dir, 'output_reasoned_csv')

        if not self.reasoner_config:
            self.reasoner_config = os.path.join(self.config_dir, "reasoner.conf")

        self.__label_map = LabelMap(self.ontology)

        self._parse_headers()

        self.lists = {}
        self.rules = []
        self._add_default_rules()
        self._parse_rules()

        self.entities = []
        self._parse_entities()
        self._parse_mapping()
        self.relations = []
        self._parse_relations()

        self._find_sparql()

    def __getattr__(self, item):
        """
        fallback if attribute isn't found
        """
        # without this, the pickling of config will fail when multiprocessing
        if item.startswith('__'):
            return super.__getattr__(item)
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

    def _parse_rules(self):
        """
        Parse rules.csv file. Used to define data validation rules.
        Expected columns are: rule,columns,level,list
        """
        file = os.path.join(self.config_dir, 'rules.csv')

        if not os.path.exists(file):
            return

        rules = []
        with open(file) as f:
            reader = csv.DictReader(f)
            rules = [d for d in reader]

        for rule in rules:
            if rule['rule'] not in VALID_RULES:
                raise AttributeError("Invalid rule in \"{}\". {} is not a valid rule [{}]".format(file, rule['rule'],
                                                                                                  ",".join(
                                                                                                      VALID_RULES)))
            if rule['rule'] == 'ControlledVocabulary':
                if not rule['list']:
                    raise AttributeError(
                        "Invalid rule in \"{}\". ControlledVocabulary rule must specify a list".format(file))

                self._parse_list(rule['list'])

            if not rule['columns']:
                raise AttributeError("Invalid rule in \"{}\". All rules must specify columns.".format(file))
            else:
                rule['columns'] = [c.strip() for c in rule['columns'].split('|')]

            if not rule['level']:
                rule['level'] = 'warning'

        self.rules.extend(rules)

    def _parse_list(self, file_name):
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
                        r['defined_by'] = self._get_uri_from_label(r['defined_by'])
                        self.lists[file_name].append(r)

    def _add_default_rules(self):
        list_name = 'phenophase_descriptions.csv'
        self.rules.append({
            'rule': 'ControlledVocabulary',
            'columns': ['phenophase_name'],
            'level': 'error',
            'list': list_name
        })

        self._parse_list(list_name)

    def _parse_entities(self):
        """
        Parse entity.csv file. Used to define the entities for triplifying
        Expected columns are: alias,concept_uri,unique_key,identifier_root
        """
        file = os.path.join(self.base_dir, 'entity.csv')

        if not os.path.exists(file):
            raise RuntimeError("entity.csv file missing from configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            self.entities = [d for d in reader]

        for entity in self.entities:
            if not entity['alias'] or not entity['concept_uri'] or not entity['unique_key']:
                raise RuntimeError(
                    "Invalid entity in {}. alias, concept_uri, and unique_key are required for each entity"
                    "listed.".format(file))

            entity['concept_uri'] = self._get_uri_from_label(entity['concept_uri'])
            entity['columns'] = []

    def _parse_mapping(self):
        """
        Parse mapping.csv file, and add the columns to the specified entities. Used to define the
        mapping of the data csv to entities for triplifying.
        Expected columns are: column,entity_alias
        """
        file = os.path.join(self.config_dir, 'mapping.csv')

        if not os.path.exists(file):
            raise RuntimeError("mapping.csv file missing from configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)

            for mapping in reader:
                entity = self.get_entity(mapping['entity_alias'])

                if not entity:
                    raise RuntimeError('Invalid entity_alias "{}" defined in {}'.format(mapping['entity_alias'], file))

                entity['columns'].append((mapping['column'], self._get_uri_from_label(mapping['uri'])))

    def _parse_relations(self):
        """
        Parse relations.csv file. Used to define the relations between entities for triplifying
        Expected columns are: subject_entity_alias,predicate,object_entity_alias
        """
        file = os.path.join(self.config_dir, 'relations.csv')

        if not os.path.exists(file):
            raise RuntimeError("relations.csv file missing from configuration directory")

        with open(file) as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            self.relations = [d for d in reader]

        for r in self.relations:
            if not r['subject_entity_alias'] or not r['predicate'] or not r['object_entity_alias']:
                raise RuntimeError(
                    "Invalid relation in {}. subject_entity_alias, predicate, and object_entity_alias are required for "
                    "each relation listed.".format(file))

            r['predicate'] = self._get_uri_from_label(r['predicate'])

    def _get_uri_from_label(self, def_text):
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

    def _parse_headers(self):
        file = os.path.join(self.config_dir, 'headers.csv')

        if os.path.exists(file):
            with open(file) as f:
                reader = csv.reader(f, skipinitialspace=True)
                self.headers = next(reader)

    def _find_sparql(self):
        self.reasoned_sparql = os.path.join(self.config_dir, 'fetch_reasoned.sparql')

        if not os.path.exists(self.reasoned_sparql):
            logging.warning(
                "did not find fetch_reasoned.sparql in config directory. will not convert reasoned data to csv")
            return
