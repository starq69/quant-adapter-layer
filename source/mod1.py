#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
Quant2018 eoddata adapter
'''
import os, errno, sys, logging
import re, fnmatch
import json   
import collections 
import quant_policy as PY

this = sys.modules[__name__]

this.log = None
this.resource_mapper_template = collections.OrderedDict() 
this.provider_path = None


def init(conf):

    '''logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    '''
    this.log = logging.getLogger(__name__)
#    this.log('...starting adapter')

    '''this.ds_root = config.get_attr(__name__, '_root')
    '''
    this.name = 'OHLCV adapter'
    this.name = conf['GLOBALS']['adapter_name']
    this.ds_root = conf['GLOBALS']['datasource_root']
    this.ds_root.strip()
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
    _list_dict_mappers = []
    try:
        for k, f in enumerate(mappers):
            #print('<<<<<' + str(f) + '>>>>>>')
            if path is not None: fn = path + '/' + f
            else: fn = f
            with open(fn) as json_mapper:
                try:
                    dict_mapper = json.load(json_mapper)
                    _list_dict_mappers.append(dict_mapper)

                except json.JSONDecodeError as e:
                    log.error('[load_resource_mappers_ex] JSONDecodeError : check syntax')

                except Exception as e:
                    log.exception('[load_resource_mappers_ex] Exception (UNMANAGED) : {}'.format(e))

    except Exception as e:
        log.exception('exception (UNMANAGED) : {}'.format(e))

    return _list_dict_mappers


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
    restituice il nodo parent (corrispondente al parent folder) se esiste
    '''
    rec = re.compile(r'(.+)/\w+')

    while path:
        match = rec.match(path)
        if match:
            _parent = match.group(1)
            #print('back=<{}>'.format(back))

            if _parent in tree:
                return tree[_parent]
            else:
                path = _parent
                continue
        else:
            break
        ###@starq69:TEST
        #return None
    return None


def get_file_items(path, pattern='*', sort=True, fullnames=False):

    if fullnames:
        _items = [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]
    else:
        _items = [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]

    if sort:
        _items=sorted(_items)
    return _items


def load_schema(path, scan_policy=None):
    '''
    ex tree_mappers()

    genera un inverted tree contenente le definizioni(dict items)  dei resource_mappers (la def. dello schema relativo a path)

    '''
    parent = tree = {}

    for node, _, _ in os.walk(path):

        #print('--parent = ' + str(parent))
        #print('--root = <{}>'.format(root))

        mappers = load_resource_mappers_ex(get_file_items(node, 'resource_mapper.*.json', fullnames=True))
        
        if not parent:
            parent = tree[node] = {PY._maps_: mappers, 'parent': {}}
            #continue
            if scan_policy == PY._SCHEMA_DS_ROOT_ONLY_:
                return tree
        else:
            parent = get_parent_ex(node, tree)
            if parent:
                tree[node] = {PY._maps_: mappers, 'parent': parent}
            else:
                tree[node] = {PY._maps_: mappers, 'parent': {}}
        
    return tree


def connect(name, resource_mapper=this.resource_mapper_template, default=False):

    class Connection():

        resource_mappers    = None     # da rimuovere
        ds                  = None     # ex _ds
        schema              = None      

        #def __init__(self, data_source, resource_mappers):
        def __init__(self, data_source, resource_mappers, mod=None): ###@Eperimental : mod == supplementary policy module (connection/data_source policy)

            self.log = logging.getLogger(__name__)
            
            # experimental
            if mod.present:
                log.info('Connection.__init__ : supplementary policy LOADED')

            ''' queste 2 variabili sono legate tra loro: la connect deve creare l'istanza solo se schema[root] == data_source_root
            in questo modo è superfluo fare il controllo:

                    if self.ds in self.schema:

            dove necessario nei metodi
            ma:
            ho definito una policy + applicata dalla check_schema_integrity()

            '''
            self.ds         = data_source
            self.schema     = resource_mappers

            #self.log.debug('Connection.__init__() : SCHEMA = <<{}>>'.format(self.schema))


        def verify_schema(self, expr):
            if PY._verify_schema_integrity_:
                if not expr: return False
            return True

        def check_schema_integrity(self, key=None, schema=None, **other):
            '''
            TBD: si può definire su policy.py ? 
            (in questo caso il modulo sarebbe parte integrante della connessione o dell'adapter ?) 
            della connessione, provare poi su policy.py alias connection-policy.py ad importare
            adapter-policy.py con le policy dell'adapter

            se le policy diventano molte e si vuole configurare un json si potrebbe autogenerare il
            relativo modulo .py prima che venga importato...

            if 'OVERRIDE_POLICY' in conf:
                build_override_module(conf['OVERRIDE_POLICY']
                try:
                    import override_policy

                module override_policy.py
                =========================
                PY.some_policy = new_value

            POLICY FLAG 1
            '''
            if PY._verify_schema_integrity_:
                if key and schema:
                    if not key in schema:
                        return False
                else: pass  # return internal error (bad parameters)
            '''
             POLICY FLAG 2
             if PY._policy_check_2_:
            '''
            
            return True


        def select(self, query):
            self.log.info('Running select({})'.format(query))
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

            self.log.info('Running ingest({})'.format(resource if resource else '<None>'))
            self.log.debug('self.ds = {}'.format(self.ds)) ###

            if resource is not None:
                try:
                    if os.path.isdir(resource):
                        '''
                        ingest directory

                        TBD: aggiungere una policy x la/le estensioni dei files
                        '''
                        #_files = [f.path for f in os.scandir(resource) if (f.is_file()) ] # (follow sym links)
                        _files = get_file_items(resource, '*.txt')
                        self.log.debug('files to ingest : {}'.format(_files))
                       
                    elif os.path.is_file(resource):
                        '''
                        ingest file
                        '''
                        self.log.debug('file to ingest : {}'.format(resource))

                except OSError as e:
                    pass
            else:
                try:
                    '''
                    ingest files in default ingest directory
                    NB
                    ATTUALMENTE FUNZIONA SOLO SE LO SCHEMA E' STATO GENRATO CON POLICY=PY._SCHEMA_DS_ROOT_ONLY_ (SINGOLO NODO ROOT)

                    PER IMPLEMENTARE LA MODALITÀ ALL_SUBFOLDERS POTREBBE ESSERE NECESSARIA UNA LISTA DEI NODI TERMINALI 
                    (QUELLI CHE NON RISULTANO MAI PARENT)
                    VALUTARE ANCHE SE UTILIZZARE UN OrderedDict nella load_schema() in modo tale che la root sia sempre il primo nodo
                    '''
                    _igst_path = self.ds + '/raw/'

                    _files = get_file_items(_igst_path, '*.txt', fullnames=True)
                    log.debug('_files = {}'.format(_files))
                    '''
                    for i, (k, v) in enumerate(self.schema.items()):
                        print("index: {}, key: {}, value: {}".format(i, k, v))
                    '''
                    if self.check_schema_integrity(key=self.ds, schema=self.schema): ### (if self.ds in self.schema:) 

                        _schema_root = self.schema[self.ds]
                        _maps = _schema_root['mappers']
                        _prepending_path_rex = '.+/'
                        for k, fn in enumerate(_files):
                            for v in _maps:
                                _regex = v['ingest']['regex']
                                _rec = re.compile(_prepending_path_rex + _regex)
                                _match = _rec.match(fn)

                                if _match is not None:
                                    self.log.debug('<{}> is a VALID ingest file'.format(fn))
                                    _market = _symbol = _timeframe = _timestamp = None
                                    groups = v['ingest']['gmatch'] ### proposed new name for groups : fields

                                    for j, K in enumerate(groups):
                                        #self.log.debug('g[{}] = {}'.format(j, g))
                                        if K == 'MKT':
                                            _market = _match.group(j+1)
                                        elif K == 'timestamp':
                                            _timestamp = _match.group(j+1)

                                    self.log.debug('_market = {}'.format(_market))
                                    self.log.debug('_timestamp = {}'.format(_timestamp))

                                else:
                                    self.log.debug('<{}> is a NOT VALID ingest file'.format(fn))
                    else:
                        print('NO KEY FOUND')


                    '''
                    ###-OLD-BEGIN
                    _igst_path = self.ds + '/raw/' # (self.ds + 'raw/') ===> TBD from configuration
                    log.debug('ingest --> : _igst_path = <{}>'.format(_igst_path))
                    #_files = [f.path for f in os.scandir(_igst_path) if (f.is_file() and fnmatch.fnmatch(f.name, '*.txt'))] # fnmatch '*.txt' ===> from config
                    _files = get_file_items(_igst_path, '*.txt') #TBD: aggiungere policy x file ext. (come sopra)

                    if _files:
                        #self.log.debug('files to ingest : {}'.format(_files))
                        _prepending_path_rex = '.+/' # necessario solo se rec = re.compile(...) usa f.path

                        for k, fn in enumerate(_files):
                            # TBD
                            # La struttura che detiene i mappers è tree quindi + che un loop si accede a tree con key=path e si ottengono i criteri
                            # definiti nei _mappers se ci sono, altrimenti si risale tree fino alla root alla ricerca di criteri validi (sezioni ingest applicabili ad fn).
                            rec = re.compile(_prepending_path_rex + self.resource_mappers[0]['ingest']['regex']) # regex from resource_mapper
                            match = rec.match(fn)

                            if match is not None:   # valid ingest file found (key value not yet verified)
                                self.log.debug('<{}> is a VALID ingest file'.format(fn))
                                _market = _symbol = _timeframe = _timestamp = None
                                groups = self.resource_mappers[0]['ingest']['gmatch'] # TBD : loop over _resource_mappers[]

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
                    ###-OLD-END
                    '''
                except FileNotFoundError as e:
                    self.log.error('ingest --> FileNotFoundError : {}'.format(e))


    '''
    connect()
    TBD
    deve tener traccia delle connessioni: stessa istanza se già creata <eoddata.com> 
    '''
    this.log.info('Running connect({})'.format(name))

    #data_source_root = this.ds_root + '/data/' + name # + '/' 
    data_source_root = this.ds_root 

    #_resource_scan_policy = PY._SCHEMA_ALL_SUBFOLDERS_
    _resource_scan_policy = PY._SCHEMA_DS_ROOT_ONLY_

    _schema = {}
    ###
    try:
        if (os.path.isdir(data_source_root)):

            if _resource_scan_policy == PY._SCHEMA_DS_ROOT_ONLY_ or _resource_scan_policy == PY._SCHEMA_ALL_SUBFOLDERS_:

                _schema = load_schema(data_source_root, _resource_scan_policy)

        else:
            log.warning('datasource <{}> NOT found!'.format(name))
            return None

    except OSError as e:
        log.error('connect --> OSError : {}'.format(e))
        return None

    '''
    qui l'istanza di Connection ha certamente adapter.type = conf.adapter

    if conf['supplementary_policy']: import test_nod as supplementary_policy
        return Connection(data_source_root, _schema, supplementary_policy)

        try:
            import supplementary_policy as SP
        except ImportError: ###
            supplementary_policy = None
    '''
    import test_nod as T # supplementary policy (connection policy)
    return Connection(data_source_root, _schema, T)

    ###
    '''
    if _resource_scan_policy == PY._SCHEMA_DS_ROOT_ONLY_:
        try:
            if (os.path.isdir(data_source_root)):
                log.info('datasource <{}> found'.format(name))    
                log.info('segue load_resource_mapper({})'.format(data_source_root))
                #_local_resource_mappers = load_resource_mappers(data_source_root)

                _tree = tree_mappers(data_source_root, _resource_scan_policy)

            else:
                log.warning('datasource <{}> NOT found!'.format(name))
                return None
        except OSError as e :
            log.error('connect --> OSError : {}'.format(e))
            return None

    elif _resource_scan_policy == PY._SCHEMA_ALL_SUBFOLDERS_:
        try:    
            if (os.path.isdir(data_source_root)):
                #ex _local_resource_mappers = load_resource_mappers(data_source_root)
                _tree = tree_mappers(data_source_root) ###@starq69: data_source_root NON deve contenere lo slash finale 
                #data_source_schema = get_schema(data_source_root)

            else:
                log.warning('datasource <{}> NOT found!'.format(name))
                return None
        except OSError as e :
            log.error('connect --> OSError : {}'.format(e))
            return None

    return Connection(data_source_root, _tree)
    #return Connection(data_source_root, data_source_schema) 
    #return Connection(data_source_root, _local_resource_mappers)
    '''
    ###

def register_resource_mapper(path, dict_mapper=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))
    try:
        with open(path + 'resource_mapper.json', 'w') as f:
                json.dump(dict_mapper, f, indent=4) 
        return True
    except OSError as e:
        log.error('register_resource_mappers --> OESError : '.format(e))
        return False


def subfolders(path):

    if path:
        try:
            _subfolders = [f.name for f in os.scandir(path) if f.is_dir() ] 
            return _subfolders
        except Exception as e:
            log.error('subfolders({}) --> exception: {}'.format(path, e))

    return None

