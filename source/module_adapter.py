#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import os, sys, logging
import json   
import collections 

this = sys.modules[__name__]

this.log = None
this.resource_mapper_template = collections.OrderedDict() 
'''
this.resource_mapper_template = {
                                'name'      : 'undef',
                                'format'    : ['@SYM', '<date>', '<open>', '<high>', '<low>', '<close>', '<vol>'],
                                'sep'       : ',',
                                'filename'  : ['@MKT', '_', '@TIMESTAMP', '.txt'],
                                'timeframe' : 'd',
                                }
'''
def init(config):

    '''logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    '''
    this.log = logging.getLogger(__name__)

    '''this.DSRoot = config.get_attr(__name__, 'DSRoot')
    '''
    this.name = 'my base module adapter'
    this.DSRoot = config
    this.resource_mapper_template['name'] = 'undef'
    this.resource_mapper_template['format'] = ['@SYM', '<date>', '<open>', '<high>', '<low>', '<close>', '<vol>']
    this.resource_mapper_template['sep'] = ','
    this.resource_mapper_template['filename'] = ['@MKT', '_', '@TIMESTAMP', '.txt']
    this.resource_mapper_template['timeframe'] = 'd'


def dataSources():
    '''
    es.
    DSRoot subfolders list
    '''
    subfolders = [f.name for f in os.scandir(this.DSRoot) if f.is_dir() ] 

    return subfolders


def ingest(keys=[], resource=None):
    pass


def register_provider(name, resource_mapper, default=False):
    pass


def register_resource_mapper(path, resource_mapper_dict=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))

    with open(path + 'resource_mapper.json', 'w') as f:
            json.dump(resource_mapper_dict, f, indent=4) 


def load_resource_mapper(path):
    pass


def select(query):
    '''
    if query in cache:
        return cache(query)
    elif ingest(query):
        refresh(cache)
        return cache(query)
    else:
        'NO data found'
    '''
    pass
