# -*- coding: utf-8 -*-
import importlib
import os
import logging
import shutil
import pandas as pd

import requests


def clean_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)


def loadClass(python_path,class_name):
    """
    dynamically loads a class given a dotted python path
    :param python_path:
    :return:
    """
    # old code
    #module_name, class_name = python_path.rsplit('.', 1)
    #mod = importlib.import_module(module_name)
    #return getattr(mod, class_name)

    spec = importlib.util.spec_from_file_location(class_name, python_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, class_name)


def fetch_query_fetcher(query_fetcher_path, repo_url):
    if not os.path.exists(query_fetcher_path):
        split_path = query_fetcher_path.rsplit('/', 1)
        jar = split_path[-1]
        _fetch_dependency(query_fetcher_path, repo_url, jar)


def fetch_ontopilot(ontopilot_path, repo_url):
    if not os.path.exists(ontopilot_path):
        split_path = ontopilot_path.rsplit('/', 1)
        jar = split_path[-1]
        _fetch_dependency(ontopilot_path, repo_url, jar)


def _fetch_dependency(output_path, repo_url, jar):
    if not repo_url.endswith('/'):
        repo_url = repo_url + '/'

    s = input('Could not find dependency: `{}`. Would you like to download this dependency from '
              '`{}`? (y/n):'.format(jar, repo_url + jar))

    if s.lower() not in ['y', 'yes']:
        logging.info('missing dependency `{}` in directory `{}`\n exiting...'.format(jar, output_path.rplit('/', 1)[0]))
        exit()

    _download_file(repo_url + jar, output_path)


def _download_file(url, output_path):
    logging.debug('downloading...\t' + url)
    r = requests.get(url, stream=True)

    if r.status_code is not 200:
        logging.error('error downloading {}: status_code: {}\n{}'.format(url, r.status_code, r.content))
        exit()

    with open(output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)

    r.close()

# An elaborate function to safely check for null values in cases that 
# A Zero (0,0.0) is not Null
# An empty string "" is a NULL
# An string or any other number is not Null
def isNull(val):
    if (not pd.isnull(val) and val == 0) or (not pd.isnull(val) and val):
        return False
    else:
        return True


