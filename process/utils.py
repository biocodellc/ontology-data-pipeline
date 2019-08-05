# -*- coding: utf-8 -*-
import importlib
import pprint
import os
import sys
import logging
import shutil
import pandas as pd

import requests


def clean_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)


def loadClass(python_path, class_name):
    """
    dynamically loads a class given a dotted python path
    :param python_path:
    :return:
    """

    spec = importlib.util.spec_from_file_location(class_name, python_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return getattr(mod, class_name)

# An elaborate function to safely check for null values in cases that 
# A Zero (0,0.0) is not Null
# An empty string "" is a NULL
# An string or any other number is not Null
def isNull(val):
    if (not pd.isnull(val) and val == 0) or (not pd.isnull(val) and val):
        return False
    else:
        return True


