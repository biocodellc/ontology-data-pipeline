# -*- coding: utf-8 -*-
import csv
import re
import logging

import multiprocessing
import pandas as pd


class InvalidData(Exception):
    pass


class Validator(object):
    """
    wrapper class that allows us to perform parallel validation on multiple files while still
    being able to ensure unique value rules
    """

    def __init__(self, config):
        self.config = config
        manager = multiprocessing.Manager()

        self.unique_values_tracker = manager.dict()
        self._init_tracker()  # to track unique values across multiple files
        self.lock = manager.RLock()

        with open(self.config.invalid_data_file, 'w') as f:
            csv.writer(f).writerow(self.config.headers)

    def validate(self, data_frame):
        return DataValidator(data_frame, self.config, self.unique_values_tracker, self.lock).validate()

    def _init_tracker(self):
        for rule in self.config.rules:
            if rule['rule'].lower() == 'uniquevalue':
                for col in rule['columns']:
                    self.unique_values_tracker[col] = []


class DataValidator(object):
    """
    performs basic data validation. Ensures that the data adheres to the specified rules
    """

    def __init__(self, df, config, unique_values_tracker, lock):
        self.config = config
        self.data = df
        self.unique_values_tracker = unique_values_tracker
        self.invalid_data = pd.DataFrame()
        self.lock = lock

    def validate(self):
        return self._validate_data()

    def _validate_data(self):
        valid = True

        for rule in self.config.rules:
            rule_name = rule['rule'].lower()

            if rule_name == 'requiredvalue':
                if not self._required_value_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'uniquevalue':
                if not self._unique_value_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'controlledvocabulary':
                if not self._controlled_vocab_rule(rule['columns'], rule['level'], rule['list']):
                    valid = False
            elif rule_name == 'integer':
                if not self._integer_rule(rule['columns'], rule['level']):
                    valid = False
            elif rule_name == 'float':
                if not self._float_rule(rule['columns'], rule['level']):
                    valid = False

        if len(self.invalid_data) > 0:
            logging.debug("dropping invalid data")
            with open(self.config.invalid_data_file, 'a') as f:
                self.invalid_data.to_csv(f, index=False, header=False)

            if self.config.drop_invalid:
                # remove all rows that are in the invalid_data DataFrame
                self.data.drop(self.invalid_data.index, inplace=True)
                valid = len(self.data) > 0

        return valid

    def _required_value_rule(self, columns, error_level):
        valid = True

        for col in columns:
            invalid_data = self.data.loc[self.data[col].isnull()]

            if len(invalid_data) > 0:
                self._log_error("Value missing in required column `{}`".format(col), error_level)

                self.invalid_data = self.invalid_data.append(
                    invalid_data.drop(self.invalid_data.index, errors='ignore'))
                if error_level.lower() == 'error':
                    valid = False

        return valid

    def _unique_value_rule(self, columns, error_level):
        valid = True

        for col in columns:
            invalid_data = pd.DataFrame()

            # check for duplicates in current data_frame
            dups = [g for _, g in self.data.groupby(col) if len(g) > 1]

            # check for duplicates in existing values. we need a lock here so only 1 processes can check for duplicates
            # AND add their values at a time
            with self.lock:
                past_values = self.unique_values_tracker[col]

                global_dups = self.data[self.data[col].isin(past_values)]

                # add current data_frame col values to tracker. to have changes propagated to multiple processes,
                # we need to reassign the dict key as changes to the list values are not tracked
                past_values.extend(self.data[col].tolist())
                self.unique_values_tracker[col] = past_values

            if len(dups) > 0:
                invalid_data = pd.concat(dups)

            if len(global_dups) > 0:
                # merge global_dups into invalid_data
                invalid_data = invalid_data.append(global_dups.drop(invalid_data.index, errors='ignore'))

            if not invalid_data.empty:
                self._log_error("Duplicate values {} in column `{}`".format(invalid_data[col].unique(), col),
                                error_level)

                self.invalid_data = self.invalid_data.append(
                    invalid_data.drop(self.invalid_data.index, errors='ignore'))
                if error_level.lower() == 'error':
                    valid = False

        return valid


    def _controlled_vocab_rule(self, columns, error_level, list_name):
        valid = True

        list = self.config.lists[list_name]
        list_values = [i['field'] for i in list]
        for col in columns:
            invalid_data = self.data.loc[~self.data[col].isin(list_values)]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self._log_error(
                    "Value `{}` in column `{}` is not in the controlled vocabulary list `{}`".format(val, col,
                                                                                                     list_name),
                    error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def _integer_rule(self, columns, error_level):
        valid = True

        for col in columns:
            # pandas can't store ints along floats and strings. The only way to coerce to ints is to drop all strings
            # and null values. We don't want to do this in the case of a warning. We will need to do the coercion later

            # returns rows where value isn't an int, ignoring empty values
            invalid_data = self.data[self.data[col].apply(
                lambda x: False if not x or pd.isnull(x) or re.fullmatch(r"[+-]?\d+(\.0+)?", str(x)) else True
            )]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self._log_error("Value `{}` in column `{}` is not an integer".format(val, col), error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def _float_rule(self, columns, error_level):
        # This will convert all numbers to floats if possible, otherwise return a the value
        mapper = lambda x: float(x) if re.fullmatch(r"[+-]?\d+(\.\d+)?", str(x)) else x

        valid = True

        for col in columns:
            # first coerce values to floats if possible
            self.data[col] = self.data[col].apply(mapper)

            # returns rows where value isn't a float, ignoring empty values
            invalid_data = self.data[
                self.data[col].apply(lambda x: False if not x or pd.isnull(x) or isinstance(x, float) else True)]

            self.invalid_data = self.invalid_data.append(invalid_data.drop(self.invalid_data.index, errors='ignore'))
            for val in invalid_data[col]:
                self._log_error("Value `{}` in column `{}` is not a float".format(val, col), error_level)

                if error_level.lower() == 'error':
                    valid = False

        return valid

    def _log_error(self, msg, level):
        logging.info("{}: {}".format(level.upper(), msg))
