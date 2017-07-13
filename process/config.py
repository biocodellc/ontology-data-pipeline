# -*- coding: utf-8 -*-

import os
import csv

VALID_RULES = ['RequiredValue', 'ControlledVocabulary', 'UniqueValue', 'Integer', 'Float']


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
        self.config_dir = os.path.join(base_dir, "config")
        self.lists = {}
        self.rules = []
        self.__parse_rules()
        self.__parse_pheno_descriptions()
        self.__add_default_rules()

    def __getattr__(self, item):
        """
        fallback if attribute isn't found
        """
        return None

    def __parse_rules(self):
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
        if not self.lists[file_name]:
            file_path = os.path.join(self.config_dir, file_name)

            if not os.path.exists(file_path):
                raise AttributeError(
                    "Invalid rule. Can't find specified list \"{}\"".format(file_path))

            with open(file_path) as file:
                reader = csv.reader(file)
                self.lists[file_name] = [r[0].strip() for r in reader]

    def __parse_pheno_descriptions(self):
        file = os.path.join(self.config_dir, 'phenophase_descriptions.csv')

        if not os.path.exists(file):
            self.rules = []
            return

        with open(file) as f:
            reader = csv.DictReader(f)
            self.pheno_descriptions = {r['field']: r['defined_by'] for r in reader}

    def __add_default_rules(self):
        list_name = 'phenophase_names'
        self.rules.append({
            'rule': 'ControlledVocabulary',
            'columns': ['phenophase_name'],
            'level': 'error',
            'list': list_name
        })

        self.lists[list_name] = [f for f in self.pheno_descriptions]
