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
    cfg_file = parent_dir + '/app.ini'
    cfg_log  = parent_dir + '/log.ini'

    try:
        logging.config.fileConfig (cfg_log)
        log = logging.getLogger (__name__)

    except Exception as e:
        print ('EXCEPTION during logging setup -> system stopped : {}'.format(e))
        sys.exit(1)

    try:
        app_config = configparser.ConfigParser ()

        if not app_config.read (cfg_file):          ### Return list of successfully read files
            log.error('missing app configuration file <{}> : ABORT....'.format(cfg_file))
            sys.exit(1)

        log.debug('app/session configuration file <{}> loaded'.format(cfg_file))

    except configparser.Error as e:
        log.error ('INTERNAL ERROR : {}'.format (e))
        log.error ('ABORT')
        sys.exit(1)

    try:
        #with Connection (app_config) as ds:
        with Connection (dict(app_config.items('GLOBALS'))) as ds:  

            log.info('connection ready!')
            ds.select('xxx')
            ds.ingest()
    except Exception as e:
        log.error('{}'.format(e))
        sys.exit(1)

if __name__ == '__main__':
    main()
