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
        self.invalid_data = pd.DataFrame()

    def validate(self):
        self.__validate_columns()
        return self.__validate_data()

    def __validate_columns(self):
        data_columns = self.data.columns.values.tolist()

        for col in AbstractPreProcessor.headers:
            if col not in data_columns:
                raise InvalidData("Missing required column: `{}`".format(col))

        return True

    def __validate_data(self):
        valid = True

        for rule in self.config.rules:
            rule_name = rule['rule']

            if rule_name == 'RequiredValue':
                if not self.__required_value_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'UniqueValue':
                if not self.__unique_value_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'ControlledVocabulary':
                if not self.__controlled_vocab_rule(rule['columns'], rule['level'], rule['list']):
                    valid = False
            elif rule_name == 'Integer':
                if not self.__integer_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'Float':
                if not self.__float_rule(rule['columns'], rule['level']):
                    valid = False

        if len(self.invalid_data) > 0:
            self.invalid_data.to_csv(self.config.invalid_data_file.name, index=False)

            if self.config.drop_invalid:
                # remove all rows that are in the invalid_data DataFrame
                self.data.drop(self.invalid_data.index, inplace=True)
                valid = len(self.data) > 0

        return valid

    def __required_value_rule(self, columns, error_level):
        valid = True

        for col in columns:
            invalid_data = self.data.loc[self.data[col].isnull()]

            if len(invalid_data) > 0:
                self.__log_error("Value missing in required column `{}`".format(col), error_level)

                self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
                if error_level.lower() == 'error':
                    valid = False

        return valid

    def __unique_value_rule(self, columns, error_level):
        valid = True

        for col in columns:
            dups = [g for _, g in self.data.groupby(col) if len(g) > 1]

            if len(dups) > 0:
                print(dups)
                invalid_data = pd.concat(dups)

                self.__log_error("Duplicate values {} in column `{}`".format(invalid_data[col].unique(), col),
                                 error_level)

                self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
                if error_level.lower() == 'error':
                    valid = False

        return valid

    def __controlled_vocab_rule(self, columns, error_level, list_name):
        valid = True

        list = self.config.lists[list_name]
        list_values = [i['field'] for i in list]
        for col in columns:
            invalid_data = self.data.loc[~self.data[col].isin(list_values)]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self.__log_error(
                    "Value `{}` in column `{}` is not in the controlled vocabulary list `{}`".format(val, col,
                                                                                                     list_name),
                    error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def __integer_rule(self, columns, error_level):
        # lambda x: int(float(x)) if re.fullmatch("[+-]?\d+(\.0+)?", str(x)) else x

        valid = True

        for col in columns:
            # pandas can't store ints along floats and strings. The only way to coerce to ints is to drop all strings
            # and null values. We don't want to do this in the case of a warning. We will need to do the coercion later

            # returns rows where value isn't an int, ignoring empty values
            invalid_data = self.data[self.data[col].apply(
                lambda x: False if not x or pd.isnull(x) or re.fullmatch("[+-]?\d+(\.0+)?", str(x)) else True
            )]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self.__log_error("Value `{}` in column `{}` is not an integer".format(val, col), error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def __float_rule(self, columns, error_level):
        # This will convert all numbers to floats if possible, otherwise return a the value
        mapper = lambda x: float(x) if re.fullmatch("[+-]?\d+(\.\d+)?", str(x)) else x

        valid = True

        for col in columns:
            # first coerce values to floats if possible
            self.data[col] = self.data[col].apply(mapper)

            # returns rows where value isn't a float, ignoring empty values
            invalid_data = self.data[
                self.data[col].apply(lambda x: False if not x or pd.isnull(x) or isinstance(x, float) else True)]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self.__log_error("Value `{}` in column `{}` is not a float".format(val, col), error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def __log_error(self, msg, level):
        print("{}: {}".format(level.upper(), msg), file=self.config.log_file)
