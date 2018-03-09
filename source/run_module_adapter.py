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

    adapter = importModule(module_name)
    
    try: 
        adapter.init(conf)

    except AttributeError as e:
        log.error('error: {}'.format(e))

    return adapter

if __name__ == '__main__':

    base_dir   = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.split(base_dir)[0]

    #conf = '/home/starq/REP/DATA/FINANCE/Quotazioni/'
    conf = parent_dir 

    logfmt='%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logfmt) 
    log = logging.getLogger(__name__)

    log.info('parent_dir = {}'.format(parent_dir))


    try:
        adapter = load_adapter(conf, 'module_adapter')

        log.info('adapter <' + adapter.name + '> ready')

        adpath = adapter.register_provider('eoddata.com')
        #adapter.register_resource_mapper(conf + '/data/')
        adapter.load_resource_mappers(adpath)

    except AttributeError as e:
        log.error('error: {}'.format(e))

    

