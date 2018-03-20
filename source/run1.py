#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
import logging, logging.config, configparser

from loader import load_adapter


if __name__ == '__main__':

    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]

    cfg_file = parent_dir + '/config.ini'
    cfg_log  = parent_dir + '/log.ini'

    # TBD : config = merge_policy(cfg_file)
    config = configparser.ConfigParser()

    if config.read(cfg_file):
        try:
            logging.config.fileConfig(cfg_log)
            log = logging.getLogger(__name__)

        except configparser.ParsingError as e:
            print('EXCEPTION : {}'.format(e))
            sys.exit(0)

    else:
        log.debug('missing <' + cfg_file + '> configuration file: STOP')
        sys.exit(0)

    log.info('config={}'.format(config))

    try:
        adapter = load_adapter(config, 'mod1')  # 'ohlcv_adapter'

        log.info('adapter <' + adapter.name + '> ready')

        appdata = adapter.connect('eoddata.com')

        appdata.select('my query')

        appdata.ingest()

    except AttributeError as e:
        log.error('error: {}'.format(e))

