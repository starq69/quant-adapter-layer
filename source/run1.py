#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys, logging
from loader import load_adapter


if __name__ == '__main__':

    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]

    #conf = '/home/starq/REP/DATA/FINANCE/Quotazioni/'
    conf = parent_dir 

    logfmt='%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logfmt) 
    log = logging.getLogger(__name__)

    log.info('conf={}'.format(conf))

    try:
        adapter = load_adapter(conf, 'mod1')  # 'ohlcv_adapter'

        log.info('adapter <' + adapter.name + '> ready')


        
        appdata = adapter.connect('eoddata.com')
        #adapter.load_resource_mappers(eoddata_path)

        appdata.select('my query')
        appdata.ingest()

    except AttributeError as e:
        log.error('error: {}'.format(e))

    

