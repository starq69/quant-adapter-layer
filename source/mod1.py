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

def load_resource_mapper(mapper, path=None):
    '''
    ARGS
        mapper : resource mapper
    RETURN
        dict
    '''
    if path is not None: mapper = path + '/' + mapper

    with open(mapper) as json_mapper:
        try:
            resource_mapper = json.load(json_mapper)
            return resource_mapper
        except json.JSONDecodeError as e:
            log.error('[load_resource_mappers] JSONDecodeError : check syntax')
        except Exception as e:
            log.exception('[load_resource_mappers] Exception (UNMANAGED) : {}'.format(e))

    return None


def load_resource_mappers_ex(mappers, path=None):
    '''
    ARGS
        path    : path 
        mappers : lista di resource mapper (file names)
    '''
    _local_resource_mappers = []
    try:
        for k, f in enumerate(mappers):
            #print('<<<<<' + str(f) + '>>>>>>')
            if path is not None: fn = path + '/' + f
            with open(fn) as json_mapper:
                try:
                    resource_mapper = json.load(json_mapper)
                    _local_resource_mappers.append(resource_mapper)
                except json.JSONDecodeError as e:
                    log.error('[load_resource_mappers_ex] JSONDecodeError : check syntax')
                except Exception as e:
                    log.exception('[load_resource_mappers_ex] Exception (UNMANAGED) : {}'.format(e))
    except Exception as e:
        log.exception('exception (UNMANAGED) : {}'.format(e))

    return _local_resource_mappers


def load_resource_mappers(path):
    '''
    ARGS
    path : folder da quale ottenere i resource_mappers
    '''
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
                        #log.debug('_local_resource_mappers[{}] : {}'.format(j, resource_mapper))

                    except json.JSONDecodeError as e:
                        log.error('load_resource_mappers --> JSONDecodeError : check syntax')

                    except Exception as e:
                        log.exception('load_resource_mappers --> exception (UNMANAGED) : {}'.format(e))  #log.error('exception : ', exc_info=True
            else:
                log.warning('invalid resource mapper file name : {}'.format(f))

    except Exception as e:
        log.exception('exception (UNMANAGED) : {}'.format(e))


    #log.debug('_local_resource_mappers loaded = {}'.format(len(_local_resource_mappers)))
    return _local_resource_mappers


def get_parent_ex(path, tree):
    '''
    restituice il nodo corrispondente al parent folder se esiste
    '''
    rec = re.compile(r'(.+)/\w+')

    while path:
        match = rec.match(path)
        if match:
            back = match.group(1)
            #print('back=<{}>'.format(back))

            if back in tree:
                return tree[back]
            else:
                path = back
                continue
        else:
            break
        
        return None


def get_file_items(path, pattern='resource_mapper.*.json', sort=True):
    _items = [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]
    if sort:
        _items=sorted(_items)
    return _items


def tree_mappers(path):
    '''
    rinominare in get_schema() ?

    genera la struttura ad albero con root = path contenente le definizioni dei resource_mappers in ordine di priorità

    '''
    parent = tree = {}

    for node, _, _ in os.walk(path):

        #print('--parent = ' + str(parent))
        #print('--root = <{}>'.format(root))
        '''
        NB : se get_file_items_ex() restituisse i files comprensivi del path potrei eliminare il par.node dalla load_resource_mapper_ex()
        '''
        #mappers = get_file_items(node, 'resource_mapper.*.json', sort=True)
        #mappers = load_resource_mappers_ex(mappers, node)

        mappers = load_resource_mappers_ex(get_file_items(node, 'resource_mapper.*.json', sort=True), node)
        
        #print('mappers = {}'.format(mappers))

        if not parent:
            parent = tree[node] = {'mappers':mappers, 'parent': {}}
            continue
        else:
            parent = get_parent_ex(node, tree)
            if parent:
                tree[node] = {'mappers':mappers, 'parent':parent}
            else:
                tree[node] = {'mappers':mappers, 'parent': {}}
        
    return tree

def connect(name, resource_mapper=this.resource_mapper_template, default=False):

    _local_resource_mappers = []

    class Connection():
        def __init__(self, data_source, resource_mappers):
            self.log = logging.getLogger(__name__)
            self._ds = data_source
            self._resource_mappers = resource_mappers
            self._tree_ds_structure = resource_mappers
            self.log.debug('Connection.tree_ds_structure : {}'.format(self._tree_ds_structure))

        def select(self, query):
            self.log.debug('select({})'.format(query))
            #self.log.debug('connection._resource_mappers = {}'.format(self._resource_mappers))
            '''
            if query in cache:
                return cache(query)
            elif ingest(query):
                refresh(cache)
                return cache(query)
            else:
                'NO data found'
            '''
        def ingest(self, resource=None):

            def f_items(path, ext=None):
                if ext is None:
                    return [(f.path, f.name) for f in os.scandir(path) if (f.is_file())]
                else:
                    return [(f.path, f.name) for f in os.scandir(path) if (f.is_file() and fnmatch.fnmatch(f.name, ext))]


            self.log.debug('self._ds = {}'.format(self._ds))

            if resource is not None:
                try:
                    #
                    # ingest directory
                    #
                    if os.path.isdir(resource):
                        '''
                        TBD: aggiungere una policy x la/le estensioni dei files
                        '''
                        #_files = [f.path for f in os.scandir(resource) if (f.is_file()) ] # (follow sym links)
                        _files = get_file_items(resource, '*.txt')
                        self.log.debug('files to ingest : {}'.format(_files))
                       
                    #
                    # ingest file
                    #
                    elif os.path.is_file(resource):
                        self.log.debug('file to ingest : {}'.format(resource))

                except OSError as e:
                    pass
            else:
                try:
                    # 
                    # scan files in default ingest directory 
                    # 
                    _igst_path = self._ds + '/raw/' # (self._ds + 'raw/') ===> TBD from configuration
                    log.debug('ingest --> : _igst_path = <{}>'.format(_igst_path))
                    #_files = [f.path for f in os.scandir(_igst_path) if (f.is_file() and fnmatch.fnmatch(f.name, '*.txt'))] # fnmatch '*.txt' ===> from config
                    '''
                    TBD: aggiungere policy x file ext. (come sopra)
                    '''
                    _files = [f.path for f in os.scandir(_igst_path) if (f.is_file())] 
                    # new
                    # _files_ex = f_items(_igst_path)

                    if _files:
                        #self.log.debug('files to ingest : {}'.format(_files))
                        _prepending_path_rex = '.+/' # necessario solo se rec = re.compile(...) usa f.path

                        for k, fn in enumerate(_files):
                            '''
                            TBD
                            
                            per quanto riguarda self._resource_mappers[x] in generale x>=1 quindi è necessario un loop (for x in len(self._resource_mappers))
                            puntando all'elemento [0] mi riferisco a resource_mapper.1.json corrisp. al mapping a + alta priorità (o ..0.json tbd)

                            *** La struttura che detiene i mappers è tree quindi + che un loop si accede a tree con key=path e si ottengono i criteri
                            defini nei _mappers se ci sono, altrimenti si risale tree fino alla root alla ricerca di criteri validi (sezioni ingest applicabili ad fn).

                            ***
                            '''
                            rec = re.compile(_prepending_path_rex + self._resource_mappers[0]['ingest']['regex']) # regex from resource_mapper
                            match = rec.match(fn)

                            if match is not None:   # valid ingest file found (key value not yet verified)
                                self.log.debug('<{}> is a VALID ingest file'.format(fn))
                                _market = _symbol = _timeframe = _timestamp = None
                                groups = self._resource_mappers[0]['ingest']['gmatch'] # TBD : loop over _resource_mappers[]

                                for j, g in enumerate(groups):
                                    #self.log.debug('g[{}] = {}'.format(j, g))
                                    if g == 'MKT':
                                        _market = match.group(j+1)
                                    elif g == 'timestamp':
                                        _timestamp = match.group(j+1)

                                self.log.debug('_market = {}'.format(_market))
                                self.log.debug('_timestamp = {}'.format(_timestamp))
                                    
                            else:
                                self.log.debug('<{}> is a NOT VALID ingest file'.format(fn))
                            pass
                    else:
                        self.log.debug('NO files to ingest')
                    pass
                except FileNotFoundError as e:
                    self.log.error('ingest --> FileNotFoundError : {}'.format(e))


    ###@star69
    #
    # connect()
    #

    this.log.info('connect({})'.format(name))
    '''
    LOW PRIORITY
    deve tener traccia delle connessioni: stessa istanza se già creata <eoddata.com>
    '''
    data_source_root = this._root + '/data/' + name # + '/' 
    '''
    Policy x la scansione dei resource_mappers
    '''
    _DS_ROOT_ONLY_      = 1
    _ALL_SUBFOLDERS_    = 2
    _resource_scan_policy = _ALL_SUBFOLDERS_

    _tree = {}

    if _resource_scan_policy == _DS_ROOT_ONLY_:
        '''
        TBD: gestire questa policy sulla tree_mappers()/get_schema() in modo che restituisca un solo nodo corrisp. a <data_source_root>
        '''
        try:
            if (os.path.isdir(data_source_root)):
                log.info('datasource <{}> found'.format(name))    
                log.info('segue load_resource_mapper({})'.format(data_source_root))
                _local_resource_mappers = load_resource_mappers(data_source_root)
            else:
                log.warning('datasource <{}> NOT found!'.format(name))
                return None
        except OSError as e :
            log.error('connect --> OSError : {}'.format(e))
            return None

    elif _resource_scan_policy == _ALL_SUBFOLDERS_:
        #
        # caricare un dict con key=relative_path e value=load_resource_mappers(relative_path)
        # o : value={relative_path, parent}
        #
        try:    # scan all subfolders for every resource_mapper.json
            if (os.path.isdir(data_source_root)):
                '''
                for (full_path, _, _) in os.walk(data_source_root):
                    log.debug('full_path : {}'.format(full_path))
                    _local_resource_mappers += load_resource_mappers(full_path)
                    #log.debug('subfolders : {}'.format(subfolders))

                #_local_resource_mappers = load_resource_mappers(data_source_root)
                '''
                _tree = tree_mappers(data_source_root) ###@starq69: data_source_root NON deve contenere lo slash finale 
                '''data_source_schema = get_schema(data_source_root) '''

            else:
                log.warning('datasource <{}> NOT found!'.format(name))
                return None
        except OSError as e :
            log.error('connect --> OSError : {}'.format(e))
            return None

    return Connection(data_source_root, _tree)
    '''return Connection(data_source_root, data_source_schema) '''
    #return Connection(data_source_root, _local_resource_mappers)


def register_resource_mapper(path, dict_mapper=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))
    try:
        with open(path + 'resource_mapper.json', 'w') as f:
                json.dump(dict_mapper, f, indent=4) 
        return True
    except OSError as e:
        log.error('register_resource_mappers --> OESError : '.format(e))
        return False


def dataSources():
    '''
    es.
    _root subfolders list
    '''
    subfolders = [f.name for f in os.scandir(this._root) if f.is_dir() ] 

    return subfolders

