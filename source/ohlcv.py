#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
Quant2018 OHLCV BackEnd Engine Model
(ohlcv.py)
'''
import os, errno, sys, logging, configparser
import re, fnmatch
import json   
import collections 
#import conventions ###  TBD from conventions import model_conventions / import model_conventions 
import model_settings
from merge_settings import merge


this = sys.modules[__name__]

policy = {}
resource_mapper_template = collections.OrderedDict() ###

_open_connections = {}


def init (ds_run_settings, ds_global_settings): ###TBD: vedi merge_policy ()

    this.log = logging.getLogger(__name__)
    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}()'.format(func_name))

    #for k, v in ds_run_settings.items(): log.debug('[{}] = {}'.format(k, v))

    base_dir   = os.path.dirname (os.path.realpath(__file__))
    parent_dir = os.path.split (base_dir)[0]
    cfg_file   = parent_dir + '/' + 'ohlcv.ini'

    if not this.policy:

        try:
            config = configparser.ConfigParser ()

            if not config.read (cfg_file):          ### Return list of successfully read files
                log.error('missing model configuration file <{}> ... try to load defaults...'.format(cfg_file))

            this.policy = merge (model_settings, config, 'MODEL') 

        except configparser.Error as e:
            log.error ('configparser.Error in model.init() : {}'.format (e))
            sys.exit(1)


    _V_, _K_ = ds_run_settings, ds_global_settings

    if not _V_ [ _K_._DATASOURCE_NAME_ ] in _open_connections:
        _open_connections[ _V_ [ _K_._DATASOURCE_NAME_ ]] = {}
        _open_connections[ _V_ [ _K_._DATASOURCE_NAME_ ]] ['_KEYS_']       = _K_ 
        _open_connections[ _V_ [ _K_._DATASOURCE_NAME_ ]] ['_SETTINGS_']   = _V_

        log.info('_open_connection [ {} ] added'.format( _V_ [ _K_._DATASOURCE_NAME_ ]))
    else:
        log.info('_open_connection [ {} ] already open!'.format(_V_ [ _K_._DATASOURCE_NAME_ ]))

    log.info('<== leave {}()'.format(func_name))

    return True

''' OLD '''
def merge_policy (conventions, configuration, section=None): ###TBD: inglobare nella init()

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}({}, {})'.format(func_name, conventions, configuration))

    '''
    for each_section in configuration.sections():
        print(each_section)
        for (each_key, each_val) in configuration.items(each_section):
            print(each_key)
            print(each_val)
    '''

    if not section: section = 'MODEL' 
    else: section=section.strip() 

    if section in configuration.sections():

        log.info('section [{}] founded in configuration'.format(section))
        configuration = dict(configuration.items(section))
        if not configuration:
            log.warning('configuration section [{}] is empty : try to load defaults'.format(section))

    else:
        configuration = {}
        log.warning('No configuration section [{}] founded : try to load defaults'.format(section))


    _K_, _V_ = conventions, conventions.policy ### 

    try:

        for k, v in conventions.defaults.items():

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
    for k, v in _V_.items(): log.debug('[{}] = {}'.format(k, v)) 

    log.info('<== leave {}()'.format(func_name))

''' TB Confirm '''
def registerConnection(ds): ###TBD: proposed new name : activate/load_datasource(ds)

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}(name={})'.format(func_name, ds))

    if ds in _open_connections:
        #log.info ('connection <{}> already open!'.format(name))
        log.info ('datasource <{}> already loaded!'.format(ds))
        return _open_connections [ name ] ### si tratta della configurazione del ds
    '''
    else:
        _open_connections [ name ] = ds.configuration ... merge_policy....
    '''

    log.info('<== leave {}'.format(func_name))


def load_index(x):
    log.warning('{} not yet implemented!'.format(sys._getframe().f_code.co_name))


def load_cache(x):
    log.warning('{} not yet implemented!'.format(sys._getframe().f_code.co_name))



def load_resource_mapper (mapper, path=None):
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


def load_resource_mappers_ex (mappers, path=None):
    '''
    ARGS
        mappers : lista di resource mapper (file names)
        path    : path 
    '''
    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}()'.format(func_name))

    _list_dict_mappers = []
    try:
        for k, f in enumerate(mappers):
            ###
            if path is not None: fn = path + '/' + f
            else: fn = f
            with open(fn) as json_mapper:
                try:
                    dict_mapper = json.load (json_mapper)
                    _list_dict_mappers.append (dict_mapper)

                except json.JSONDecodeError as e:
                    log.error('{} : JSONDecodeError : check syntax on file {}'.format(func_name, fn))

                except Exception as e:
                    log.exception('{} : Unmanaged Exception! : {}'.format(func_name, e))

    except Exception as e:
        log.exception('{} Unmanaged Exception : {}'.format(func_name, e))

    log.info('<== leave {}'.format(func_name))
    return _list_dict_mappers


def get_parent (path, tree):
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


def get_file_items (path, pattern=None, sort=True, fullnames=True):

    # qui pattern può essere _INGEST_DEFAULT_FILE_PATTERN_ ....
    # ma se uso _INGEST_FILE_PATTERNS_ (lista) dovrò appendere su _items
    # ad ogni iterazione

    if not pattern: pattern = '*'

    if fullnames:
        _items = [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]
    else:
        _items = [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]

    if sort:
        _items=sorted(_items)
    return _items

''' NEW '''
def load_schema (ds_name): 

    func_name = sys._getframe().f_code.co_name                                                            
    log.info('==> Running {}({})'.format(func_name, ds_name))                
                                                                                                          
    _K_ = _open_connections[ ds_name ] ['_KEYS_']                                                         
    _V_ = _open_connections[ ds_name ] ['_SETTINGS_']                                                     

    if '_SCHEMA_' in _open_connections[ ds_name ]:
        log.waning('SCHEMA OVERRIDE...')

    _open_connections[ ds_name ] ['_SCHEMA_'] = {}                                                    
    _schema = _open_connections[ ds_name ] ['_SCHEMA_']                                                     
    data_source_root = _V_ [ _K_._DATASOURCE_ROOT_ ]                                                                  
                                                                                                          
    def _load_schema (path, scan_policy=None):

        parent = tree = {} 

        for node, _, _ in os.walk(path):                                                                      
                                                                                                              
            mappers = load_resource_mappers_ex ( get_file_items ( node, _V_ [ _K_._MAPPERS_PATTERN_STYLE_ ])) ###
            log.info('->num. mappers: {}'.format(str(len(mappers))))                                               
            
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

            _schema = _load_schema (data_source_root, _scan_policy)                                                     

        else:
            log.warning('datasource <{}> NOT found!'.format(name))
            return None

    except KeyError as ke:
        log.error('INTERNAL ERROR : 1 or more settings _SCHEMA_ key NOT DEFINED : pls check configuration & implementation')
        log.error(str(ke))
        sys.exit(0)

    except OSError as e:
        log.error('OSError : {}'.format(e))

        return None

    _open_connections[ ds_name ] ['_SCHEMA_'] = _schema

    #log.debug('+++++ SCHEMA ++++++')
    #log.debug(_schema)
    log.info('<== leave {}()'.format(func_name))                                                          


''' NEW '''
def check_schema_integrity (ds_name, key=None, schema=None, **other):

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}()'.format(func_name))

    _K_     = _open_connections[ ds_name ] ['_KEYS_']  
    _V_     = _open_connections[ ds_name ] ['_SETTINGS_'] 
    schema  = _open_connections[ ds_name ] ['_SCHEMA_']
    key     = _V_ [ _K_._DATASOURCE_ROOT_ ]
    '''
    log.debug ('_K_ = {}'.format(_K_))
    log.debug ('_V_ = {}'.format(_V_))
    log.debug ('schema = {}'.format(schema))
    log.debug ('key = {}'.format(key))

    POLICY FLAG 1
    '''
    log.debug('_VERIFY_SCHEMA_INTEGRITY_ = {}'.format( _V_[ _K_._VERIFY_SCHEMA_INTEGRITY_]))

    if _V_ [ _K_._VERIFY_SCHEMA_INTEGRITY_ ]:
        if key and schema:
            log.debug('KEY & SCHEMA')
            if not key in schema:
                log.debug('NOT KEY in SCHEMA')
                return False
        else: 
            log.debug('NOT(KEY and SCHEMA) ==> bad params')
            pass  

    '''
     POLICY FLAG 2...
    '''
    log.info('<== leave {}()'.format(func_name))
    return True


''' NEW '''
def _ingest (ds_name, _files):

    func_name = sys._getframe().f_code.co_name
    log.info('==> Running {}(ds_name={})'.format(func_name, ds_name))

    _K_     = _open_connections[ ds_name ] ['_KEYS_']  
    _V_     = _open_connections[ ds_name ] ['_SETTINGS_'] 
    _schema = _open_connections[ ds_name ] ['_SCHEMA_']

    '''
    for i,/ (k, v) in enumerate(self.schema.items()):
        print("index: {}, key: {}, value: {}".format(i, k, v))
    '''

    if check_schema_integrity (ds_name, key=_V_ [ _K_._DATASOURCE_ROOT_], schema=_schema): 
    
        _schema_root = _schema [ _V_ [ _K_._DATASOURCE_ROOT_ ] ]
        _maps        = _schema_root [ _K_._MAPS_ ]  
        log.debug('_maps ------> {}'.format(_maps))

        _prepending_path_rex = '.+/' # necessaria poichè _files[x] = path/nome_file mentre la regex è relativa solo a nome_file

        _idx        = {}
        _mkt_dict   = {}
        _sym        = {} 

        #for k, fn in enumerate (_files):
        for _map in _maps:

            _regex = _map [ _K_._INGEST_ ] [ _K_._REGEX_ ] # new
            _rec = re.compile (_prepending_path_rex + _regex)
            keys    = _map [ _K_._INGEST_ ] [ _K_._GMATCH_ ]  

            #for _map in _maps:
            for k, fn in enumerate (_files):

                _match = _rec.match (fn)

                if _match is not None:
                    log.debug('<{}> is a VALID ingest file'.format(fn))

                    _market = _symbol = _timeframe = _timestamp = None      ### KEYS

                    for j, key in enumerate (keys):
                        '''
                        skey = _match.group(j+1)
                        if skey == key:
                            # get deep_level(key) 
                            # deep = _V_ [ key.join('_DEEP_') ]
                            log.debug(

                            # l'idea è quella di ottenere dai global_settings un attributo di key (essendo mappabile in base al nome)
                            # che rappresenta il livello e il tipo di dato relativo


                        '''
                        if key == '@MKT':

                            _market = _match.group(j+1)
                            if _market not in _idx:

                                _mkt_dict = _idx[_market]   = {}
                                #_mkt_dict       = _idx[_market]

                        elif key == '@SYM':
                            pass

                        elif key == '@timeframe':
                            pass

                        elif key == '@timestamp':
                            _timestamp = _match.group(j+1)

                    log.info('Keys founded in file : market = {}, timestamp = {}'.format(_market, _timestamp))
                    log.debug('we proced to analyze file content...')

                    fields      = _map [ _K_._INGEST_ ] [ _K_._FORMAT_ ]
                    separator   = _map [ _K_._INGEST_ ] [ _K_._SEPARATOR_ ]

                    with open(fn, 'r') as f:
                        rows    = f.read().splitlines()
                        header  = rows.pop(0)
                        log.debug('HEADER : {}'.format(header))
                        for row in rows:
                            data = row.split( separator )

                            for i, field in enumerate(data):
                                if fields[i][:1] == '@': ### iskey()
                                    if not field in _mkt_dict:
                                        ###TBD:
                                        # debbo rimuovere field da data...
                                        _mkt_dict [ field ] = data
                                    #log.debug('key {} found : {}'.format(fields[i], field))

                        log.debug('tot rows = {}'.format(len(rows)))
                    
#                    for k, _map in sorted(_mkt_dict.items()):
#                        log.debug ('_mkt_dict : {} -> {}'.format(k, _map))

                else:
                    log.warning('<{}> is a NOT VALID ingest file'.format(fn))
    else:
        log.error('check_schema_integrity : FALSE (bad keys/not found / internal error')
        ###raise

    log.info('<== leave {}()'.format(func_name))


def iskey (field):
    if field[:1] == '@': return True
    else: return False

#https://stackoverflow.com/questions/10399614/accessing-value-inside-nested-dictionaries
def get_nested(data, *args):
    if args and data:
        element  = args[0]
        if element:
            value = data.get(element)
            return value if len(args) == 1 else get_nested(value, *args[1:])


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

