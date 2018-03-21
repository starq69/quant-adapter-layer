#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys
import logging, logging.config, configparser

from loader import load_adapter

def _start():
    '''
    init log/config

    if KO: raise exception

    return config, log
    '''
    pass

def _connect(adapter_name, data_source_name):
    pass
    '''
    try:
        adapter = load_adapter(config, adapter_name)  # 'ohlcv_adapter'

        log.info('adapter <' + adapter.name + '> ready')

        data_source = adapter.connect(data_source_name)

    except:
        #TBD
        pass
    return data_source # (ex appdata)
    '''

if __name__ == '__main__':

    '''
    try:
        conf, log = _start()

        ds = connect (adapter, data_source)

        data = ds.select(query)

    except:
        # quit
    '''

    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]

    cfg_file = parent_dir + '/config.ini'
    cfg_log  = parent_dir + '/log.ini'

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

