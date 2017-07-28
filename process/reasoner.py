# -*- coding: utf-8 -*-
import subprocess
import os

CUR_DIR = os.path.join(os.path.dirname(__file__))


def run_reasoner(input_file, output_file, config_file):
    subprocess.check_call(['java', '-jar', os.path.join(CUR_DIR, '../lib/ontopilot.jar'),
                           '-i', input_file,
                           '-o', output_file,
                           '-c', config_file,
                           'inference_pipeline'])
