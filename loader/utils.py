# -*- coding: utf-8 -*-
import os


def get_files(dir, ext='csv'):
    for root, dirs, files in os.walk(dir):

        if len(files) == 0:
            print("no files found in {}".format(dir))

        for file in files:
            if file.endswith(ext):
                yield os.path.join(root, file)
