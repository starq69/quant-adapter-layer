#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
import logging, logging.config, configparser
from class_adapter import Connection
#from module_adapter import connect #, select, ingest

def main():

    base_dir   = os.path.dirname (os.path.realpath(__file__))
    parent_dir = os.path.split (base_dir)[0]
    cfg_file = parent_dir + '/config.ini'
    cfg_log  = parent_dir + '/log.ini'
    config = configparser.ConfigParser ()

    if config.read (cfg_file): 
        try:
            logging.config.fileConfig (cfg_log)
            log = logging.getLogger (__name__)
        except configparser.ParsingError as e:
            print ('EXCEPTION : {}'.format (e))
            sys.exit(0)
    else:
        print ('missing <' + cfg_file + '> configuration file: STOP')
        sys.exit(1)

    with Connection(config, 'ohlcv adapter', 'eoddata.com') as ds:
        log.info('connection ready!')
        ds.select('xxx')

if __name__ == '__main__':
    main()
