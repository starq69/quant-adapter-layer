#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys, logging
import click
from importFromURI import importModule 
'''
@click.command()
@click.argument('adapter')
def start(module_name):
    pass
'''

def load_adapter(conf, module_name):
    '''
    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]
    root_dir = base_dir
    '''

    adapter = importModule(module_name)
    
    try: 
        adapter.init(conf)

    except AttributeError as e:
        log.error('error: {}'.format(e))

    return adapter

if __name__ == '__main__':

    conf = '/home/starq/REP/DATA/FINANCE/Quotazioni/'

    logfmt='%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logfmt) 
    log = logging.getLogger(__name__)

    try:
        adapter = load_adapter(conf, 'module_adapter')

        log.info('adapter <' + adapter.name + '> ready')

        log.info(adapter.dataSources())

    except AttributeError as e:
        log.error('error: {}'.format(e))

    

