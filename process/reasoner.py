# -*- coding: utf-8 -*-
import logging
import subprocess
import os
import re

CUR_DIR = os.path.join(os.path.dirname(__file__))

def run_reasoner(input_file, output_file, config_file, robot_path):

    logging.debug("reasoning on file {}".format(input_file))

    # the java version is unreliable and does not provide useful debugging output
    cmd = ['java', '-jar', robot_path, 'reason', '-r', 'elk', '--axiom-generators', '"InverseObjectProperties ClassAssertion"', '-i', input_file, '--include-indirect', 'true', '--exclude-tautologies','structural','reduce','-o', output_file]

    logging.debug("running reasoner with: ")
    logging.debug(subprocess.list2cmdline(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    # take filename from stdout from process, decode and clean up output
    output = str(stdout.decode('utf-8')).replace('writing ','').replace('\n','').replace(CUR_DIR,'').lstrip()

    if not os.path.exists(output_file):
        #raise RuntimeError("Failed to perform reasoning on {}".format(input_file) + ".  " + output)
        logging.debug("Failed to perform reasoning on {}".format(input_file) + ".  " + output)

    # provide docker friendly output (this way user looks for file in relative path home environment instead of docker mount)
    cleanfilename = re.sub('^%s' % '/process/', '', output_file)
    logging.info('reasoned output at ' + cleanfilename)
