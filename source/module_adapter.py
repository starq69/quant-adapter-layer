#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import os, errno, sys, logging
import fnmatch
import json   
import collections 

this = sys.modules[__name__]

this.log = None
this.resource_mapper_template = collections.OrderedDict() 
this.provider_path = None

this.started=False

def init(conf):

#    if not this.started:

    '''logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    '''
    this.log = logging.getLogger(__name__)
#    this.log('...starting adapter')

    '''this.DSRoot = config.get_attr(__name__, 'DSRoot')
    '''
    this.name = 'my base module adapter'
    this.DSRoot = conf
    this.resource_mapper_template['name'] = 'undef'
    this.resource_mapper_template['format'] = ['@SYM', 'date', 'open', 'high', 'low', 'close', 'vol']
    this.resource_mapper_template['sep'] = ','
    this.resource_mapper_template['filename'] = ['@MKT', '_', '@TIMESTAMP', '.txt']
    this.resource_mapper_template['timeframe'] = 'd'

#    this.started=True
#    else:
#    this.log.info('...adapter already started')


def dataSources():
    '''
    es.
    DSRoot subfolders list
    '''
    subfolders = [f.name for f in os.scandir(this.DSRoot) if f.is_dir() ] 

    return subfolders


def ingest(keys=[], resource=None):
    pass


def register_provider(name, resource_mapper=this.resource_mapper_template, default=False):
###@starq69
#def connect(...) # class factory
#
    this.log.info('register_provider {}'.format(name))
    path = this.DSRoot + '/data/' + name + '/'
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            log.error(e)
            return False
            #raise
        else:
            log.warning('provider {} already registered on path {}'.format(name, path))
            '''
            load_resource_mappers(path)
            return get_resource_mappers(path)
            '''
    ###@starq69
    #connection = factory(..)
    #return connection

    register_resource_mapper(path, resource_mapper)
    this.provider_path = path
    return this.provider_path

###@starq69
# class factory (..):
#   self.resource_mapper
#   def select(..)
#   def ingest(..)

def register_resource_mapper(path, dict_mapper=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))
    try:
        with open(path + 'resource_mapper.json', 'w') as f:
                json.dump(dict_mapper, f, indent=4) 
        return True
    except OSError as e:
        log.error(e)
        return False


def load_resource_mappers(path):

    log.info('load_resource_mappers on path {}'.format(path))
    try:
        resource_mappers = [f.path for f in os.scandir(path) if (f.is_file(follow_symlinks=False) and fnmatch.fnmatch(f.name, '*.json'))]
        for f in resource_mappers:
            with open (f) as json_mapper:
                resource_mapper = json.load(json_mapper)
            log.info('resource_mapper (json.load) : {}'.format(resource_mapper))
    #OSError (https://docs.python.org/3/library/os.html#os.DirEntry)
    # (https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist) 
    except OSError as e: 
        log.error(e)
        return False


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
