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

        self.base_dir = base_dir
        self.config_dir = os.path.join(base_dir, "config")
        self.__parse_lists()
        self.__parse_rules()

    def __getattr__(self, item):
        """
        fallback if attribute isn't found
        """
        return None

    def __parse_rules(self):
        file = os.path.join(self.config_dir, 'rules.csv')

        if not os.path.exists(file):
            self.rules = []
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
                elif not self.lists[rule['list']]:
                    raise AttributeError(
                        "Invalid rule in \"{}\". Can't find specified list \"{}\"".format(file, rule['list']))

            if not rule['columns']:
                raise AttributeError("Invalid rule in \"{}\". All rules must specify columns.".format(file))

    def __parse_lists(self):
        file = os.path.join(self.config_dir, 'lists.csv')

        self.lists = {}
        if not os.path.exists(file):
            return

        with open(file) as f:
            reader = csv.reader(f)

            for row in reader:
                name = row[0]

                if name in self.lists:
                    raise AttributeError("Lists file contains duplicate lists with name `{}`".format(name))

                self.lists[name] = [s.strip() for s in row[1]]
