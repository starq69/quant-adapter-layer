#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
Quant2018 eoddata adapter
'''
import os, errno, sys, logging
import re, fnmatch
import json   
import collections 
import conventions ### proposed : rename to settings.py

from utils import caller_name, protected    ###

this = sys.modules[__name__]

#this.log = None
#this.conf = None
this.resource_mapper_template = collections.OrderedDict() ###

_open_connections = {}

@protected ###TBD
def merge_policy(conventions, configuration, section=None): ### per ora uso section es: section='POLICY'
    '''
    NB
    posso passare direttamente configuration ['POLICY']
    così posso utilizzare la funzione sia per l'adapter sia per la connessione :
    merge_policy (conventions, configuration ['ADAPTER'])
    merge_policy (conventions, configuration ['CONNECTION') ora 'ADAPTER' e 'CONNECTION' sono fusi in 'POLICY'
    NB
    il parametro deve essere riconoscibile come adapter/connection o altro...
    '''

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}({}, {})'.format(func_name, conventions, configuration))

    _caller = sys._getframe(1).f_code.co_name
    log.info('EXPERIMENTAL : caller = {}'.format(_caller))

    _caller = caller_name(skip=2)
    log.info('EXPERIMENTAL : caller = {}'.format(_caller))

    ###

#    try:
#        configuration = configuration['POLICY'] ### questo è da verificare a monte sulla init o sulla connect
#    except KeyError as e:
#        configuration = {}
#        log.warning('No configuration policy section founded. Try to load defaults...')

    if section == 'POLICY':
    #test
    #if section == 'POLICY' or section == 'MODEL':
        '''
        proposed : Key_, Val_ = settings, settings.configured
        '''
        _K_, _V_ = conventions, conventions.policy ### qui si copia conventions.adapter / .connection o altro in base al parametro section
        configuration = configuration['POLICY'] ###
        #configuration = configuration[section] ###

        try:

            for k, v in conventions.defaults.items():

                #log.debug('default.item : [{}] = {}'.format(k, v))
                _msg = 'conventions.default.item : [{}] = {}'.format(k, v) 

                if k in configuration:

                    log.debug(_msg + ' + found in config')
                    
                    if not (k in conventions.const):

                        log.debug('variable')

                        conventions.policy[k] = configuration[k] # new

                    elif _K_._ACCEPT_CONST_OVERRIDE_ in configuration and (configuration[_K_._ACCEPT_CONST_OVERRIDE_]): #

                        log.debug('const : config accept_const_override = True ==> variable')

                        conventions.policy[k] = configuration[k] #

                    else:

                        if conventions.defaults [ _K_._ACCEPT_CONST_OVERRIDE_ ] :
                            
                            log.debug('const : config accept_const_override = True ==> variable')
                            conventions.policy[k] = configuration[k]
                        else:

                            log.debug('accept_const_override NOT CONFIGURED')
                            conventions.policy[k] = conventions.defaults[k]

                else:
                    log.debug(_msg + ' - NOT found in config')
                    conventions.policy[k] = conventions.defaults[k]

        except Exception as e:
                    log.error('merge_policy exception : {}'.format(e))

        log.info('merged configuration :')
        #for k, v in conventions.policy.items(): log.debug('[{}] = {}'.format(k, v))
        for k, v in _V_.items(): log.debug('[{}] = {}'.format(k, v))

        #return conventions, conventions.policy

    elif section == 'MODEL':
        pass
    elif section == 'CONNECTION':
        pass
    else:
        log.warning('section parameter value="{}" not yet managed')
        return False

    log.info('<== leave {}()'.format(func_name))


def init (configuration): 

    this.log = logging.getLogger(__name__)
    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}({})'.format(func_name, configuration))

    # TBD 
    # merge_policy (conventions, configuration [ 'ADAPTER' ] ) ### la section però deve poter essere dedotta nelle merge_policy()
    #
    merge_policy (conventions, configuration, section='POLICY') # forse da ricollocare nel costruttore di Connection...

    # ...qui sicuramente :
    # carica le policy dell'adapter: dai parametri, dal file di configurazione, dai defaults
    # utilizza un set per trovare i parametri passati alla funzione, utilizza defaults per settare i parametri non configurati (sul file di conf dell'adapter/model)

    #_K_, _V_ = conventions, conventions.policy

    this.name = configuration['GLOBALS']['adapter_name']

    this.ds_root = configuration['GLOBALS']['datasource_root']  ### probabile parametro da spostare sulla connect
    this.ds_root.strip()

    log.debug('>>>type(conf) = <{}>'.format(str(type(configuration))))
    log.debug('>>>type(conf[\'GLOBALS\']) = <{}>'.format(str(type(configuration['GLOBALS']))))
    log.info('>>>conf[GLOBALS][adapter_name] = <{}>'.format(configuration['GLOBALS']['adapter_name']))

    this.resource_mapper_template['name'] = 'undef'
    this.resource_mapper_template['format'] = ['@SYM', 'date', 'open', 'high', 'low', 'close', 'vol']
    this.resource_mapper_template['sep'] = ','
    this.resource_mapper_template['filename'] = ['@MKT', '_', '@TIMESTAMP', '.txt']
    this.resource_mapper_template['timeframe'] = 'd'

    log.info('<== leave {}()'.format(func_name))


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
            log.error('[load_resource_mapper] JSONDecodeError : check syntax')
        except Exception as e:
            log.exception('[load_resource_mapper] Exception (UNMANAGED) : {}'.format(e))

    return None


def load_resource_mappers_ex(mappers, path=None):
    '''
    ARGS
        path    : path 
        mappers : lista di resource mapper (file names)
    '''

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}()'.format(func_name))

    _list_dict_mappers      = []
    try:
        for k, f in enumerate(mappers):
            ###
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

    log.info('<== leave {}'.format(func_name))
    return _list_dict_mappers


def get_parent(path, tree):
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


def get_file_items(path, pattern=None, sort=True, fullnames=True):

    if not pattern: pattern = '*'

    if fullnames:
        _items = [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]
    else:
        _items = [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]

    if sort:
        _items=sorted(_items)
    return _items

@protected
def load_schema(path, scan_policy=None):

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}({}, scan_policy={})'.format(func_name, path, scan_policy))

    _caller = caller_name(skip=2)
    log.info('EXPERIMENTAL 1 : caller = {}'.format(_caller))

    _caller = sys._getframe(1).f_code.co_name                           ### caller func name es.: <connect>
    log.info('EXPERIMENTAL 2 : caller = {}'.format(_caller))

    _caller = sys._getframe(0).f_globals.get('__name__', '__main__')    ### <mod1>
    log.info('EXPERIMENTAL 3 : caller = {}'.format(_caller))

    _caller = sys._getframe().f_back.f_code.co_filename
    log.info('EXPERIMENTAL 4 : caller = {}'.format(_caller))


    _K_, _V_ = conventions, conventions.policy

    parent = tree = {}

    for node, _, _ in os.walk(path):

        mappers = load_resource_mappers_ex ( get_file_items ( node, _V_ [ _K_._MAPPERS_PATTERN_STYLE_ ])) ###
        log.info('->mappers: {}'.format(str(len(mappers))))
        
        if not parent:
            parent = tree[node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': {}}

            if scan_policy == _V_ [ _K_._SCHEMA_DS_ROOT_ONLY_ ]: 
                return tree
        else:
            parent = get_parent(node, tree)
            if parent:
                tree[node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': parent }
            else:
                tree[node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': {}}

    log.info('<== leave {}()'.format(func_name))
    return tree


def connect (name, default=False):
    '''
    par. default attualmente not implemented
    '''
    class Connection():

        name                = None  ### oppure è : schema [_K_._DATA_SOURCE_NAME ] da caricare sulla load_schema()
        ds                  = None 
        schema              = None 

        def __init__(self, name, data_source_root, schema): ###TBD: aggiungere index / cache

            self.log = logging.getLogger(__name__)

            self.name       = name              ## univoco : si usa questa chiave per verificare sulla connect() se esiste già l'istanza in _open_connections[]
            self.ds         = data_source_root  ## rinominare in dsr
            self.schema     = schema            ##
            ##self.index    = index
            ##self.cache    = cache

            self.log.debug('Connection.__init__() : SCHEMA = <{}>'.format(self.schema))


        def check_schema_integrity(self, key=None, schema=None, **other):

            func_name = sys._getframe().f_code.co_name
            self.log.info('==> Running {}()'.format(func_name))
            _K_, _V_ = conventions, conventions.policy
            '''
            TBD: si può definire su policy.py ? 
            (in questo caso il modulo sarebbe parte integrante della connessione o dell'adapter ?) 
            della connessione, provare poi su policy.py alias connection-policy.py ad importare
            adapter-policy.py con le policy dell'adapter

            POLICY FLAG 1
            '''
            log.debug('_verify_schema_integrity_ = {}'.format( _V_[ _K_._VERIFY_SCHEMA_INTEGRITY_]))

            if _V_ [ _K_._VERIFY_SCHEMA_INTEGRITY_ ]:
                if key and schema:
                    log.debug('KEY & SCHEMA')
                    if not key in schema:
                        log.debug('NOT KEY in SCHEMA')
                        return False
                else: 
                    log.debug('NOT(KEY and SCHEMA) ==> bad params')
                    pass  # return internal error (bad parameters)

            '''
             POLICY FLAG 2...
            '''
            self.log.info('<== leave {}()'.format(func_name))
            return True


        def select(self, query):

            func_name = sys._getframe().f_code.co_name
            self.log.info('==> Running {}(resource={})'.format(func_name, query if query else '<None>'))
            '''
            if query in cache:
                return cache(query)
            elif ingest(query):
                refresh(cache)
                return cache(query)
            else:
                'NO data found'
            '''
            self.log.info('<== leave {}()'.format(func_name))


        def ingest(self, resource=None):

            func_name = sys._getframe().f_code.co_name
            self.log.info('==> Running {}(resource={})'.format(func_name, resource if resource else '<None>'))
            self.log.debug('self.ds = {}'.format(self.ds)) ###

            # experimental
            _caller = sys._getframe(1).f_code.co_name
            self.log.info('EXPERIMENTAL : caller = {}'.format(_caller)) # <module>

            _K_, _V_ = conventions, conventions.policy

            if resource is not None:
                try:
                    if os.path.isdir(resource):
                        '''
                        ingest directory
                        '''
                        self.log.debug('ingest DIRECTORY')
                        _files = get_file_items(resource, _V_ [ _K_._INGEST_DEFAULT_FILE_PATTERN_ ])

                        if not _files:
                            log.warning('NO FILES TO INGEST FOUND')
                            return None

                        self.log.debug('files to ingest : {}'.format(_files))

                        ###
                       
                    elif os.path.is_file(resource):
                        '''
                        ingest file
                        '''
                        self.log.debug('ingest FILE')
                        self.log.debug('file to ingest : {}'.format(resource))
                        ###

                except OSError as e:
                    pass
            else:
                try:
                    '''
                    ingest files in default ingest directory (x ora solo se _SCHEMA_DS_ROOT_ONLY_)
                    NB
                    PER IMPLEMENTARE LA MODALITÀ ALL_SUBFOLDERS POTREBBE ESSERE NECESSARIA UNA LISTA DEI NODI TERMINALI 
                    (QUELLI CHE NON RISULTANO MAI PARENT)
                    VALUTARE ANCHE SE UTILIZZARE UN OrderedDict nella load_schema() in modo tale che la root sia sempre il primo nodo
                    '''

                    if _V_ [ _K_._SCHEMA_SCAN_OPTION_ ] != _K_._SCHEMA_DS_ROOT_ONLY_:
                        log.debug ('Ingest from default location if scan option is not schema_ds_root_only is NOT YET IMPLEMENTED')
                        sys.exit(1)

                    log.debug('ingest from DEFAULT LOCATION')

                    _igst_path = self.ds + '/raw/'
                    _files = get_file_items (_igst_path, _V_ [ _K_._INGEST_DEFAULT_FILE_PATTERN_ ])

                    if not _files:
                        log.warning('NO FILES TO INGEST FOUND')
                        return None

                    log.debug('_files = {}'.format(_files))
                    '''
                    for i,/ (k, v) in enumerate(self.schema.items()):
                        print("index: {}, key: {}, value: {}".format(i, k, v))
                    '''
                    if self.check_schema_integrity (key=self.ds, schema=self.schema): 

                        _schema_root = self.schema[self.ds]
                        _maps = _schema_root[ _K_._MAPS_ ]  # new
                        _prepending_path_rex = '.+/'

                        for k, fn in enumerate (_files):
                            for v in _maps:
                                _regex = v [ _K_._INGEST_ ] [ _K_._REGEX_ ] # new
                                _rec = re.compile(_prepending_path_rex + _regex)
                                _match = _rec.match(fn)

                                if _match is not None:
                                    self.log.debug('<{}> is a VALID ingest file'.format(fn))
                                    _market = _symbol = _timeframe = _timestamp = None
                                    groups = v[ _K_._INGEST_ ] [ _K_._GMATCH_ ]  # new


                                    for j, K in enumerate (groups):
                                        #self.log.debug('g[{}] = {}'.format(j, g))
                                        if K == 'MKT':
                                            _market = _match.group(j+1)
                                        elif K == 'timestamp':
                                            _timestamp = _match.group(j+1)

                                    self.log.info('market = {}, timestamp = {}'.format(_market, _timestamp))

                                else:
                                    self.log.warning('<{}> is a NOT VALID ingest file'.format(fn))
                    else:
                        log.error('check_schema_integrity : FALSE (bad keys/not found / internal error')
                        ###raise

                except FileNotFoundError as e:
                    self.log.error('FileNotFoundError : {}'.format(e))

            self.log.info('<== leave {}()'.format(func_name))

    '''
    connect
    '''

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}(name={})'.format(func_name, name))

    if name in _open_connections:
        log.info ('connection <{}> already open!'.format(name))
        return _open_connections [ name ]

    data_source_root = this.ds_root 

    _K_, _V_ = conventions, conventions.policy

    _schema = {}

    try:
        if (os.path.isdir(data_source_root)):

            if  not _V_ [_K_._SCHEMA_SCAN_OPTION_]  in _K_._SCHEMA_SET:
                log.error('BAD CONFIGURATION : schema_scan_option={}'.format(_V_ [_K_._SCHEMA_SCAN_OPTION_]))
                ###raise 
                sys.exit(0) ### TBD

            if _V_ [ _K_._SCHEMA_SCAN_OPTION_ ] == _V_ [ _K_._SCHEMA_DS_ROOT_ONLY_ ]:

                _scan_policy = _K_._SCHEMA_DS_ROOT_ONLY_

            elif _V_ [ _K_._SCHEMA_SCAN_OPTION_ ] == _V_ [ _K_._SCHEMA_ALL_SUBFOLDERS_ ]:

                _scan_policy = _K_._SCHEMA_ALL_SUBFOLDERS_

            _schema = load_schema (data_source_root, _scan_policy)

        else:
            log.warning('datasource <{}> NOT found!'.format(name))
            return None

    except KeyError as ke:
        log.error('INTERNAL ERROR : 1 or more settings _SCHEMA_ key NOT DEFINED : pls check configuration & implementation')
        log.error(str(ke))
        sys.exit(0)

    except OSError as e:
        log.error('connect --> OSError : {}'.format(e))
        return None

    try:
        this._open_connections [ name ] = Connection (name, data_source_root, _schema)
        return this._open_connections [ name ]
    except Exception as e:
        log.debug('some kind of problems here : {}'.format(e))
        return None


def register_resource_mapper (path, dict_mapper=this.resource_mapper_template):

    this.log.info('register_resource_mapper on path {}'.format(path))
    try:
        with open(path + 'resource_mapper.json', 'w') as f:
                json.dump(dict_mapper, f, indent=4) 
        return True
    except OSError as e:
        log.error('register_resource_mappers --> OESError : '.format(e))
        return False


def subfolders (path):

    if path:
        try:
            _subfolders = [f.name for f in os.scandir(path) if f.is_dir() ] 
            return _subfolders
        except Exception as e:
            log.error('subfolders({}) --> exception: {}'.format(path, e))

    return None

