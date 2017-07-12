# -*- coding: utf-8 -*-

import re
import pandas as pd
from preprocessor import AbstractPreProcessor


class InvalidData(Exception):
    pass


class Validator(object):
    """
    performs basic data validation. Ensures that the data adheres to the specified rules and contains all of the columns
    specified in the preprocess.AbstractPreProcessor.headers list
    """

    def __init__(self, config, df):
        self.config = config
        self.data = df

    def validate(self):
        return self.__validate_columns() and self.__validate_data()

    def __validate_columns(self):
        data_columns = self.data.columns.values.tolist()

        for col in AbstractPreProcessor.headers:
            if col not in data_columns:
                raise InvalidData("Missing required column: `{}`".format(col))

        return True

    def __validate_data(self):
        error = False

        for rule in self.config.rules:
            rule_name = rule['rule']

            if rule_name == 'RequiredValue':
                if not self.__required_value_rule(rule['columns'], rule['level']):
                    error = True
            elif rule_name == 'UniqueValue':
                if not self.__unique_value_rule(rule['columns'], rule['level']):
                    error = True
            elif rule_name == 'ControlledVocabulary':
                if not self.__controlled_vocab_rule(rule['columns'], rule['level'], rule['list']):
                    error = True
            elif rule_name == 'Integer':
                if not self.__integer_rule(rule['columns'], rule['level']):
                    error = True
            elif rule_name == 'Float':
                if not self.__float_rule(rule['columns'], rule['level']):
                    error = True

        return error

    def __required_value_rule(self, columns, error_level):
        error = False

        for col in columns:
            invalid_data = self.data.loc[self.data[col].isnull()]

            if len(invalid_data) > 0:
                self.__log_error("Value missing in required column `{}`".format(col), error_level)

                if self.config.drop_invalid:
                    # remove all rows that are in the invalid_data DataFrame
                    self.data = self.data[~self.data.index.isin(invalid_data.index)]
                elif error_level.lower() == 'error':
                    error = True

        return error

    def __unique_value_rule(self, columns, error_level):
        error = False

        for col in columns:
            invalid_data = pd.concat(g for _, g in self.data.groupby(col) if len(g) > 1)

            if len(invalid_data) > 0:
                self.__log_error("Duplicate values in column `{}`".format(col), error_level)

                if self.config.drop_invalid:
                    # remove all rows that are in the invalid_data DataFrame
                    self.data = self.data[~self.data.index.isin(invalid_data.index)]
                elif error_level.lower() == 'error':
                    error = True

        return error

    def __controlled_vocab_rule(self, columns, error_level, list_name):
        error = False

        list = self.config.lists[list_name]
        for col in columns:
            invalid_data = self.data.loc[~self.data[col].isin(list)]

            for val in invalid_data[col]:
                self.__log_error("Value `{}` is not in the controlled vocab list: `{}`".format(val, list_name),
                                 error_level)

                if self.config.drop_invalid:
                    # remove all rows that are in the invalid_data DataFrame
                    self.data = self.data[~self.data.index.isin(invalid_data.index)]
                elif error_level.lower() == 'error':
                    error = True

        return error

    def __integer_rule(self, columns, error_level):
        # the float is necessary for the case of 0.0. This will convert all numbers to ints if possible, otherwise
        # return a str. does not perform any rounding
        mapper = lambda x: int(float(x)) if re.fullmatch("[+-]?\d+(\.0+)?", str(x)) else str(x)

        error = False

        for col in columns:
            # first coerce values to ints if possible
            self.data[col] = self.data[col].apply(mapper)

            # returns rows where value isn't an int
            invalid_data = self.data[self.data[col].apply(lambda x: False if not x or isinstance(x, int) else True)]

            for val in invalid_data[col]:
                self.__log_error("Value `{}` is not an integer".format(val), error_level)

                if self.config.drop_invalid:
                    # remove all rows that are in the invalid_data DataFrame
                    self.data = self.data[~self.data.index.isin(invalid_data.index)]
                elif error_level.lower() == 'error':
                    error = True

        return error

    def __float_rule(self, columns, error_level):
        # the float is necessary for the case of 0.0. This will convert all numbers to ints if possible, otherwise
        # return a str. does not perform any rounding
        mapper = lambda x: float(x) if re.fullmatch("[+-]?\d+(\.\d+)?", str(x)) else str(x)

        error = False

        for col in columns:
            # first coerce values to ints if possible
            self.data[col] = self.data[col].apply(mapper)

            # returns rows where value isn't a float
            invalid_data = self.data[self.data[col].apply(lambda x: False if not x or isinstance(x, float) else True)]

            for val in invalid_data[col]:
                self.__log_error("Value `{}` is not an integer".format(val), error_level)

                if self.config.drop_invalid:
                    # remove all rows that are in the invalid_data DataFrame
                    self.data = self.data[~self.data.index.isin(invalid_data.index)]
                elif error_level.lower() == 'error':
                    error = True

        return error

    def __log_error(self, msg, level):
        print("{}: {}".format(level.upper(), msg), file=self.config.log_file)
