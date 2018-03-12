#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import os, errno, sys, logging
import re, fnmatch
import json   
import collections 

this = sys.modules[__name__]

this.log = None
this.resource_mapper_template = collections.OrderedDict() 
this.provider_path = None

def init(conf):
    '''logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    '''
    this.log = logging.getLogger(__name__)
#    this.log('...starting adapter')

    '''this._root = config.get_attr(__name__, '_root')
    '''
    this.name = 'OHLCV adapter'
    this._root = conf
    this.resource_mapper_template['name'] = 'undef'
    this.resource_mapper_template['format'] = ['@SYM', 'date', 'open', 'high', 'low', 'close', 'vol']
    this.resource_mapper_template['sep'] = ','
    this.resource_mapper_template['filename'] = ['@MKT', '_', '@TIMESTAMP', '.txt']
    this.resource_mapper_template['timeframe'] = 'd'

def load_resource_mappers(path):
    _mappers = [f.path for f in os.scandir(path) if (f.is_file(follow_symlinks=False) and fnmatch.fnmatch(f.name, 'resource_mapper.*.json'))]
    _local_resource_mappers = []
    try:
        for k, f in enumerate(_mappers):
            log.debug('_mappers[{}] : <{}>'.format(k, f))
            id_ref=re.match(r'.+.(\d+).json', f)
            if id_ref is not None:
                j = int(id_ref.group(1))
                
                with open (f) as json_mapper:
                    try:
                        resource_mapper = json.load(json_mapper)
                        _local_resource_mappers.insert(j, resource_mapper)
                        log.info('_local_resource_mappers[{}] : {}'.format(j, resource_mapper))

                    except json.JSONDecodeError as e:
                        log.error('JSONDecodeError : check syntax')

                    except Exception as e:
                        log.exception('exception (UNMANAGED) : {}'.format(e))  #log.error('exception : ', exc_info=True
            else:
                log.warning('unknown resource mapper : {}'.format(f))

    except Exception as e:
        log.error('exception (UNMANAGED) : {}'.format(e))


    log.debug('_local_resource_mappers loaded = {}'.format(len(_local_resource_mappers)))


def connect(name, resource_mapper=this.resource_mapper_template, default=False):

    class Connection():
        def __init__(self, conf):
            self._root = conf
        def select(self, query):
            this.log.info('select({})'.format(query))
            '''
            if query in cache:
                return cache(query)
            elif ingest(query):
                refresh(cache)
                return cache(query)
            else:
                'NO data found'
            '''
        def ingest(self, datastore):
            # ingest(keys=[], resource=None):
            this.log.info('ingest({})'.format(datastore))

    this.log.info('connect({})'.format(name))
    '''
    deve tener traccia delle connessioni: stessa istanza se già creata <eoddata.com>
    '''
    data_source= this._root + '/data/' + name + '/'
    try:
        if (os.path.isdir(data_source)):
            log.info('datasource <{}> found'.format(name))    
            log.info('segue load_resource_mapper({})'.format(data_source))
            load_resource_mappers(data_source)
        else:
            log.warning('datasource <{}> NOT found!'.format(name))
            return None
    except OSError as e :
        log.error('OSError : {}'.format(e))
        return None


    '''
    ###@starq69: crea datasource se non esiste (valutare anche altre modalità)
    #
    # n.b. qui si verifica l'integrità della risorsa 'data_source'
    # si caricano gli indici e la cache
    # i riferimenti così ottenuti possono essere passati al costrutture di Connection() sulla return
    try:
        os.makedirs(data_source)
    except OSError as e:
        if e.errno != errno.EEXIST:
            log.error(e)
            return False
        else:
            log.info('datasource founded on path {}'.format(data_source))

    register_resource_mapper(data_source, resource_mapper)
    '''
    return Connection(data_source)

    # si crea la connessione al data_source
    # return Connection(data_source, resource_mapper)


def register_resource_mapper(path, dict_mapper=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))
    try:
        with open(path + 'resource_mapper.json', 'w') as f:
                json.dump(dict_mapper, f, indent=4) 
        return True
    except OSError as e:
        log.error(e)
        return False

'''
def load_resource_mappers(path):

    log.info('load_resource_mappers on path {}'.format(path))
    try:
        resource_mappers = [f.path for f in os.scandir(path) if (f.is_file(follow_symlinks=False) and fnmatch.fnmatch(f.name, 'resource-mapper.*.json'))]
        for f in resource_mappers:
            id_ref=re.search('.+.(\d+).json', f)
            if id_ref is not None:
                log.debug('id_ref= {}'.format(id_ref.group(0)))
                with open (f) as json_mapper:
                    resource_mapper = json.load(json_mapper)
                    #resource_mapper[id_ref.group(0)] = json.load(json_mapper)

                log.info('resource_mapper (json.load) : {}'.format(resource_mapper))

    #OSError (https://docs.python.org/3/library/os.html#os.DirEntry)
    # (https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist) 
    except OSError as e: 
        log.error(e)
        return False
'''


def dataSources():
    '''
    es.
    _root subfolders list
    '''
    subfolders = [f.name for f in os.scandir(this._root) if f.is_dir() ] 

    return subfolders

