# -*- coding: utf-8 -*-
import requests

from loader.utils import get_files

DEFAULT_ENDPOINT = "http://localhost:9999/blazegraph/namespace/{}/sparql"


def load(input_dir, destination=DEFAULT_ENDPOINT):
    for input_file in get_files(input_dir, '.ttl'):
        data = open(input_file, 'rb').read()

        if destination == DEFAULT_ENDPOINT:
            namespace = "".join(destination.rsplit('.', 1)[0])
            destination.format(namespace)

        res = requests.post(destination, data=data, headers={'Content-Type:text/turtle'})

        if res.status_code != 200:
            print(res.content)
