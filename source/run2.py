#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
import logging, logging.config, configparser
from class_adapter import Connection

'''
main module (run2.py)
'''
def main():

    base_dir   = os.path.dirname (os.path.realpath(__file__))
    parent_dir = os.path.split (base_dir)[0]
    cfg_file = parent_dir + '/config.ini'
    cfg_log  = parent_dir + '/log.ini'

    try:

        logging.config.fileConfig (cfg_log)
        log = logging.getLogger (__name__)

    except Exception as e:
        print ('EXCEPTION during logging setup -> system stopped : {}'.format(e))
        sys.exit(1)

    try:
        config = configparser.ConfigParser ()

        if not config.read (cfg_file):          ### Return list of successfully read files
            log.error('missing <{}> configuration file: STOP'.format(cfg_file))
            sys.exit(1)

    except configparser.Error as e:
        print ('STOP : {}'.format (e))
        sys.exit(1)


    with Connection(config, 'ohlcv', 'eoddata.com') as ds:
        log.info('connection ready!')
        ds.select('xxx')

if __name__ == '__main__':
    main()