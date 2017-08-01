# -*- coding: utf-8 -*-
import importlib
import os

import shutil


def clean_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)

def loadClass(python_path):
    """
    dynamically loads a class given a dotted python path
    :param python_path:
    :return:
    """
    module_name, class_name = python_path.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)


def loadPreprocessorFromProject(base_dir):
    """
    loads a PreProcessor from the specified base_dir

    Assumptions:
        1. base_dir contains a file `preprocessor.py`
        2. in this file, there is a class named *PreProcessor

    :param base_dir:
    :return:
    """
    path = os.path.join(base_dir, 'preprocessor.py')
    if not os.path.exists(path):
        raise ImportError("Error loading preprocessor. File does not exist `{}`.".format(path))

    spec = importlib.util.spec_from_file_location(path, path)
    module = spec.loader.load_module()
    try:
        return getattr(module, 'PreProcessor')
    except AttributeError:
        raise ImportError(
            "Error loading preprocessor. Could not find a PreProcessor class in the file `{}`".format(path))
