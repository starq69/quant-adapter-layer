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
from distutils.util import strtobool ## TBD: usare str2bool
from functools import lru_cache
#import conventions ###  TBD from conventions import model_conventions / import model_conventions 
import model_settings
from merge_settings import merge_settings


this = sys.modules[__name__]

policy = {}
resource_mapper_template = collections.OrderedDict() ###

_open_connections = {}


def init (ds_run_settings, ds_global_settings): ###TBD: vedi merge_policy ()

    this.log = logging.getLogger(__name__)
    func_name = sys._getframe().f_code.co_name
    log.info('>>> Running {}()'.format(func_name))

    #for k, v in ds_run_settings.items(): log.debug('[{}] = {}'.format(k, v))

    base_dir   = os.path.dirname (os.path.realpath(__file__))
    parent_dir = os.path.split (base_dir)[0]
    cfg_file   = parent_dir + '/ohlcv.ini'

    if not this.policy:

        try:
            configured = configparser.ConfigParser ()

            if not configured.read (cfg_file):          ### Return list of successfully read files
                log.error('missing model configuration file <{}> ... try to load defaults...'.format(cfg_file))

            this.policy = merge_settings (model_settings, configured, 'MODEL') 

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

    log.info('<<< leave {}()'.format(func_name))

    return True

''' TB Confirm '''
def registerConnection (ds): ###TBD: proposed new name : activate/load_datasource(ds)

    func_name = sys._getframe().f_code.co_name
    log.info('>>> Running {}(name={})'.format(func_name, ds))

    if ds in _open_connections:
        #log.info ('connection <{}> already open!'.format(name))
        log.info ('datasource <{}> already loaded!'.format(ds))
        return _open_connections [ name ] ### si tratta della configurazione del ds
    '''
    else:
        _open_connections [ name ] = ds.configuration ... merge_policy....
    '''

    log.info('<<< leave {}'.format(func_name))


def load_index(tbd):
    log.warning('{} not yet implemented!'.format(sys._getframe().f_code.co_name))


def load_cache(tbd):
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
    log.info('>>> Running {}()'.format(func_name))

    _list_dict_mappers = []
    try:
        for k, f in enumerate(mappers):
            ###
            if path is not None: fn = path + '/' + f
            else: fn = f
            with open(fn) as json_mapper:
                try:
                    dict_mapper = json.load (json_mapper)
                    # aggiunge il nome file (dict_mapper ha sempre una sola key che corrisponde al nome 'logico' della risorsa)
                    dict_mapper [ list (dict_mapper) [0]] ['full-resource-name']    = str(f)
                    dict_mapper [ list (dict_mapper) [0]] ['resource-name']         = str(os.path.basename(f))
#                    dict_mapper ['__internals__'] = {}
#                    dict_mapper ['__internals__']['full-resource-name']    = str(f)
#                    dict_mapper ['__internals__']['resource-name']         = str(os.path.basename(f))
                    _list_dict_mappers.append (dict_mapper)

                except json.JSONDecodeError as e:
                    log.error('{} : JSONDecodeError : check syntax on file {}'.format(func_name, fn))

                except Exception as e:
                    log.exception('{} : Unmanaged Exception! : {}'.format(func_name, e))

    except Exception as e:
        log.exception('{} Unmanaged Exception : {}'.format(func_name, e))

    log.info('<<< leave {}'.format(func_name))
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

    if not pattern: pattern = '*'

    _items = []

    if type (pattern) is list:

        for p in pattern:

            if fullnames:
                _items += [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, p)]
            else:
                _items += [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, p)]
    else:
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
    log.info('>>> Running {}({})'.format(func_name, ds_name))                
                                                                                                          
    _K_                 = _open_connections[ ds_name ] ['_KEYS_']                                                         
    _V_                 = _open_connections[ ds_name ] ['_SETTINGS_']                                                     
    data_source_root    = _V_ [ _K_._DATASOURCE_ROOT_ ]                                                                  

    if '_SCHEMA_' in _open_connections [ ds_name ]:
        log.waning('SCHEMA OVERRIDE!')

    # always override
    _schema = _open_connections[ ds_name ] ['_SCHEMA_'] = {}      ### empty SCHEMA 

                                                                                                          
    def _load_schema (path, scan_policy=None):
        '''
        args :
            path        : root dir to scan (ds_root)
            scan_policy : schema_ds_root_only | schema_all_subfolders_

        return :
            dict (ds_schema)
        '''
        if scan_policy == _V_ [ _K_._SCHEMA_DS_ROOT_ONLY_ ]:

            _tree = {_K_._TABLES_: {}, _K_._MAPS_: {}}

            t_definitions, t_names = validate_tables ( load_resource_mappers_ex ( get_file_items ( path, _V_[_K_._DATASET_MAP_FILES_])))  # issue#2
            if not t_definitions:
                log.error('NO TABLE DEFINITIONS FOUND! Pls check datasource configuration. ABORT')
                sys.exit(1)
            log.info('->valid datasource tables = {}'.format(t_names)) 

            _tree [_K_._TABLES_] = t_definitions
            #log.info('->tables : {}'.format(_tree[_K_._TABLES_])) 

            # issue#2 : ex _MAPPERS_PATTERN_STYLE_ 
            ingest_maps = validate_ingest_maps ( load_resource_mappers_ex ( get_file_items ( path, _V_[_K_._INGEST_MAP_FILES_])), t_names, t_definitions)
            if not ingest_maps:
                log.error('NO VALID INGEST MAPPERS FOUND! (ingestion is disabled for this session) Pls check ingest mappers definition files and try again.') #. ABORT')
                #sys.exit(1)
            else:
                _tree[_K_._MAPS_] = ingest_maps
                log.info('->tot ingest mappers found : {}'.format(len(ingest_maps))) 
                log.debug('->ingest mappers : {}'.format(_tree[_K_._MAPS_])) 

        elif scan_policy == _V_ [ _K_.SCHEMA_ALL_SUBFOLDERS_]:
            # not yet implemented
            pass
            '''
            parent = _tree = {}
            _tree = {_K_._TABLES_: [], _K_._MAPS_: {}}

            for node, _, _ in os.walk(path): 

                ingest_maps = load_resource_mappers_ex ( get_file_items ( node, _V_[_K_._INGEST_MAP_FILES_]))  # issue#2 : ex _MAPPERS_PATTERN_STYLE_ 
                log.info('->tot ingest mappers found : {}'.format(str(len(ingest_maps)))) 
                
                if not parent:                                                                                    
                    parent = _tree[_K_._MAPS_][node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': {}}                           
                    
                else:   
                    parent = get_parent(node, _tree)                                                               
                    if parent:
                        _tree[_K_._MAPS_][node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': parent }                           
                    else:
                        _tree[_K_._MAPS_][node] = { _V_ [ _K_._MAPS_ ] : mappers, 'parent': {}}                                

                t_definitions  = load_resource_mappers_ex ( get_file_items ( node, _V_[_K_._DATASET_MAP_FILES_]))  # issue#2
                log.info('->tot table definitions : {}'.format(str(len(t_definitions)))) 
                _tree [_K_._TABLES_] += t_definitions
            '''
        log.info('<<< leave {}()'.format(func_name))                                                          

        return _tree

    try:
        if (os.path.isdir(data_source_root)):

            if  not _V_ [_K_._SCHEMA_SCAN_OPTION_]  in _K_._SCHEMA_SET_:
                log.error('BAD CONFIGURATION : schema_scan_option={}'.format(_V_ [_K_._SCHEMA_SCAN_OPTION_]))
                ###TBD :raise 
                sys.exit(0) ### 

            log.debug('segue _load_schema () ....')
            _schema = _load_schema (data_source_root, _V_[_K_._SCHEMA_SCAN_OPTION_]) 

        else:
            log.error('datasource <{}> NOT found!'.format(name))
            return None

    except KeyError as ke:
        log.error('INTERNAL ERROR : 1 or more settings _SCHEMA_ key NOT DEFINED : pls check configuration & implementation')
        log.error(str(ke))
        ## raise
        sys.exit(0)

    except OSError as e:
        log.error('OSError : {}'.format(e))

        return None

    _open_connections[ ds_name ] ['_SCHEMA_'] = _schema

    #log.debug('ds_schema = {}'.format(_schema)) 
    log.info('<<< leave {}()'.format(func_name))                                                          


def validate_tables (resource_mappers):
    '''
    in  :   list of dict [{key=tablename, value=[list of fields]}]
    out :   a dict with less or equal items 

    check : duplicate #tablenames 
            [keys] not empty
            [fields] not empty
            
    vedere : https://stackoverflow.com/questions/1207406/how-to-remove-items-from-a-list-while-iterating
    '''
    _tables = {}
    _names  = []
    #_msg    = 'INVALID table definition : '  

    for i, table in enumerate (resource_mappers):
        for _, (t_name, t_definition) in enumerate (table.items()):

            _resource = t_definition ['resource-name']
            #_resource = t_definition ['__internals__']['resource-name']

            if ('keys' not in t_definition) or (not t_definition ['keys']):
                log.warning('<{}> keys not found in table <{}> (DISCARD)'.format(_resource, t_name))
                continue
            elif ('fields' not in t_definition) or (not t_definition ['fields']):
                log.warning('<{}> fields not found in table <{}> (DISCARD)'.format(_resource, t_name))
                continue

            if t_name not in _names:
                _names.append(t_name)
                log.info('<{}> define a valid table <{}>'.format(_resource, t_name))
                _tables [ t_name ] = t_definition
            else:
                log.error('DUPLICATE table definition : {} - pls resolve this problem (ABORT)'.format(t_name))
                sys.exit(1)

    return _tables, _names


def get_table_keys (table_name, t_definitions):

    for _, (t_name, t_definition) in enumerate (t_definitions.items()):
        if t_name == table_name:
            if ('keys' in t_definition):
                return t_definition ['keys']

    return []
                

def validate_ingest_maps (ingest_maps, t_names, t_definitions):
    
    _maps   = {}
    _checks = []
    i_keys  = []
    _msg    = 'INVALID mapper definition : '

    for i, _map in enumerate (ingest_maps):
        for _, (i_name, i_definition) in enumerate (_map.items()):

            _resource = i_definition ['resource-name']
            #_resource = i_definition ['__internals__']['resource-name']

            if ('fpattern' not in i_definition) or (not i_definition ['fpattern']):
                log.warning('<{}> <{}> : file pattern (fpattern) not defined (DISCARD)'.format(_resource, i_name))
                #log.warning('<{}> keys not found in table <{}>'.format(_resource, t_name))
                continue

            if ('table' not in i_definition) or (not i_definition ['table']):
                log.warning('<{}> : '.format(i_name) + _msg + '"table" not defined (DISCARD)')
                continue
            else:
                t_name = i_definition ['table']

            if t_name not in t_names:
                log.warning('<{}> table <{}> not found in SCHEMA (DISCARD)'.format(_resource, t_name))
                continue
            else:
                table_keys = get_table_keys (t_name, t_definitions) 

            if ('global-keys' not in i_definition):
                log.warning('<{}> <{}> : global-keys not defined (DISCARD)'.format(_resource, i_name))
                continue
            else:
                i_keys += i_definition ['global-keys']

            if ('local-keys' not in i_definition):
                log.warning('<{}> <{}> : local-keys not specified (DISCARD)'.format(_resource, i_name))
                continue
            else:
                i_keys += i_definition ['local-keys']

            i_keys = set(i_keys)
            '''
            log.debug('mapper keys : {}'.format(i_keys))
            log.debug('table keys : {}'.format(table_keys))
            '''
            key_err = False
            for _, t_key in enumerate (table_keys):
                if not (t_key in i_keys):
                    ### check if key has default
                    if t_key in i_definition and i_definition [t_key]:
                        log.debug('<{}> : default value <{}> founded for key <{}>'.format(i_name, i_definition[t_key], t_key))
                    else:
                        log.warning('<{}> <{}> : mandatory key <{}> NOT defined (DISCARD)'.format(_resource, i_name, t_key))
                        key_err = True
                        break
            else:
                log.debug('<{}> : keys validation OK'.format(i_name))

            if key_err: break

            _id = str(i_definition ['fpattern']) + str(i_definition ['table'])
            if _id not in _checks:
                _checks.append(_id)
                _maps [ i_name ] = i_definition
                log.info('<{}> define a valid ingest mapper <{}>'.format(_resource, i_name))
            else:
                log.error('DUPLICATE ingest mapper definition : "{}" found on {} - pls resolve this problem, ABORT'.format(_id, i_name))
                sys.exit(1)

    return _maps


@lru_cache(maxsize=64, typed=False)
def check_schema_integrity (ds_name, key=None, schema=None, **other):   ### +par : soft=True (soft/hard check - invalidate cache?)

    func_name = sys._getframe().f_code.co_name
    log.info('>>> Running {}()'.format(func_name))

    _K_     = _open_connections[ ds_name ] ['_KEYS_']  
    _V_     = _open_connections[ ds_name ] ['_SETTINGS_'] 

    if not schema : schema  = _open_connections[ ds_name ] ['_SCHEMA_']     ## default
    if not key    : key = _K_._TABLES_                                      ## default key for integrity ceck
    '''
    log.debug ('_K_     = {}'.format(_K_))
    log.debug ('_V_     = {}'.format(_V_))
    log.debug ('schema  = {}'.format(schema))
    log.debug ('key     = {}'.format(key))

    POLICY FLAG 1
    '''
    log.debug('_VERIFY_SCHEMA_INTEGRITY_ = {}'.format( _V_[ _K_._VERIFY_SCHEMA_INTEGRITY_]))

    if bool (strtobool (_V_ [ _K_._VERIFY_SCHEMA_INTEGRITY_ ])):

        if not key in schema:
            log.debug('KEY <{}> NOT in SCHEMA'.format(str(key)))
            return False

    '''
     POLICY FLAG 2...
    '''
    log.info('...No more policy to check schema integrity')
    log.info('<<< leave {}()'.format(func_name))

    return True


''' NEW '''
def _ingest (ds_name, _files):

    func_name = sys._getframe().f_code.co_name
    log.info('>>> Running {}(ds_name={})'.format(func_name, ds_name))

    _K_     = _open_connections[ ds_name ] ['_KEYS_']  
    _V_     = _open_connections[ ds_name ] ['_SETTINGS_'] 
    _schema = _open_connections[ ds_name ] ['_SCHEMA_']
    '''
    for i, (k, v) in enumerate(self.schema.items()):
        print("index: {}, key: {}, value: {}".format(i, k, v))
    '''
    if check_schema_integrity (ds_name): 
    
        '''_schema_root = _schema [ _V_ [ _K_._DATASOURCE_ROOT_ ] ]'''
        _maps        = _schema [ _K_._MAPS_ ]  
        '''log.debug('_maps ------> {}'.format(_maps))'''

        _prepending_path_rex = '.+/' # necessaria poichè _files[x] = path/nome_file mentre la regex è relativa solo a nome_file

        _idx        = {}
        _mkt_dict   = {}
        _sym        = {}

        _attr           = '_ATTR_'
        _parent         = '_PARENT_'
        _value          = '_VAL_'

        _tot_maps       = 0

        for _, (_ingest_map_name, _map_definition) in enumerate (_maps.items()):

            '''print ('_ingest_map_name : ' +_ingest_map_name)'''

            _regex      = _map_definition [ _K_._FPATTERN_ ] # new
            _rec        = re.compile (_prepending_path_rex + _regex)
            keys        = _map_definition [ _K_._KEYMATCH_ ]  ### keys associate al nomefile definite nel mapper file

            keys_attr   = {}
            for _, key in enumerate (keys): keys_attr [ key ] = _V_ [ key + str(_attr) ] ## ver. compatta senza log e indice
#            try:
#                for j, key in enumerate (keys):
#                    log.debug('enumerate keys : {}, {}'.format(j, key))
#                    keys_attr [ key ]   = _V_ [ key + str(_attr) ]
#                    log.debug('keys_attr [ {} ] = <{}>'.format(key, keys_attr [ key]))
#            except KeyError as e:
#                log.warning('WARNING : {}'.format(e))

            _data_mapper    = {}

#           try:
#               while validate_keys(_files.pop()):  ### raise NoFile
#                   _ingest_data()
#               else:
#                   log.warning('invalid keys definition for file')
#           except NoFile :
#               total time to process nnn _files is xxxx

            for k, fn in enumerate (_files):

                _match = _rec.match (fn)
                if _match is not None:
                    ''' VALID INGEST FILE/RESOURCE'''

                    log.info('<{}> is a VALID ingest file'.format(fn))
                    _market = _symbol = _timeframe = _timestamp = None      ### KEYS

                    '''
                    LETTURA KEY-VALUE DAL NOMEFILE
                    '''
                    for j, key in enumerate (keys):

                        _maybe  = ()
                        _val    = _match.group(j+1)        # valore

                        #log.debug ('BUG HERE! --> keys_attr [key] = {}'.format(keys_attr[key]))
                        #for i, v in enumerate(keys_attr):
                        #    log.debug('--> {} : {}'.format(i, v))

                        if _K_._IS_NODE_ in keys_attr [ key ]:
                            '''
                            NB. in questa fase - retrieve dei valori delle keys presenti nel nome file - le keys con attributo _IS_NODE_
                            vengono allocate regolarmente nella struttura di data_mapping (ad es. MKT o SYM)
                            '''
                            log.debug('{} key is a node key!'.format(key))

                            _data_mapper [ key ] = []  
                            _data_mapper [ key ].append({'_PARENT_KEY_':None, '_VALUE_':_val})

                        else:
                            _maybe = key, {'_PARENT_KEY_':None, '_VALUE_':_val}

                            log.debug('maybe : <{}> <{}>'.format(key, _maybe[-1]))


                        if _K_._HAS_PARENT_ in keys_attr [ key ]:
                            '''
                            NB. in questa fase - retreive dei valori delle keys presenti nel nome file - non è necessario
                            verificare la presenza dell'elemento parent key (ad es. SYM x la key timestamp), ciò significa che 
                            tale key (ad es. timestamp) potrà essere anche associata successivamente ai data point (in quanto viene 
                            comunque salvato nella sessione di data_mapping relativa al file) più verosimilmente cmq troveremo
                            di nuovo la stessa key nei data point stessi e pertanto verranno utilizzati i relativi valori presi
                            dal datapoint
                            '''
                            parent_key = _V_ [ key + str(_parent) ] 

                            log.debug('<{}> key has parent key <{}>'.format(key, parent_key))

                            if parent_key in _data_mapper:
                                if _maybe:
                                    #_maybe[-1][_value] = _data_mapper [ parent_key ]
                                    _maybe[-1]['_PARENT_KEY_'] = _data_mapper [ parent_key ]
                                else:
                                    _data_mapper [ key ]['_PARENT_KEY_'] = _data_mapper [ parent_key ]

                                log.debug('_HAS_PARENT_ attr set for key <{}> & PARENT KEY available during file name analisys!'.format(key))
                                log.debug('_data_mapper = {}'.format(_data_mapper))
                                log.debug('_maybe = {}'.format(_maybe))

                            else:
                                log.debug('_HAS_PARENT_ attr set for key <{}> & NO PARENT KEY found during file name analisys!'.format(key))

                        log.debug ('_data_mapper = {}'.format(_data_mapper))

                        # update data mapper
                        #
                        #_data_mapper [ key ] = []       ### key can be: @MKT, @SYM, @timeframe, @timestamp
                        #_data_mapper [ key ].append(_val)

                        log.debug('KEY/val/attr : {}/{}/{}'.format(key, _val, keys_attr[key]))

                        # OLD #
#                        if key == '@MKT':
#
#                            _market = _match.group(j+1) # valore
#                            if _market not in _idx:
#
#                                _mkt_dict = _idx[_market]   = {}
#                                #_mkt_dict       = _idx[_market]
#
#                        elif key == '@SYM':
#                            pass
#
#                        elif key == '@timeframe':
#                            pass
#
#                        elif key == '@timestamp':
#                            _timestamp = _match.group(j+1) # valore
                        # OLD END #

                    #log.info('Keys founded in file : market = {}, timestamp = {}'.format(_market, _timestamp))
                    log.info('we proced to analyze file content...')

                    fields      = _map_definition [ _K_._FFORMAT_ ]
                    separator   = _map_definition [ _K_._SEPARATOR_ ]

                    with open(fn, 'r') as f:
                        #rows    = f.read().splitlines()
                        #header  = rows.pop(0)

                        header = f.readline().rstrip()
                        # TEST header for keys....:ok
                        rows = f.read().splitlines()
                        # ko : warning + continue   ###
                        
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

                        log.info('tot data rows = {}'.format(len(rows)))

                    '''TBD
                    manage policy to implement one-pass ingest file elaboration or not
                    in pratica il policy flag definisce se si debba impedire o meno che i files di dati una volta elaborati possano essere rielaborati con il successivo mappers
                    if ONE_PASS:
                        ...
                    '''
                    
                    
#                    for k, _map in sorted(_mkt_dict.items()):
#                        log.debug ('_mkt_dict : {} -> {}'.format(k, _map))


                else:
                    log.warning('<{}> is NOT a VALID ingest file'.format(fn))

            _tot_maps += 1

        else:
            if _tot_maps:
                log.info('(found {} mappers)'.format(_tot_maps))
            else:
                log.warning('No mappers found : skip ingest'.format(_tot_maps))

    else:
        log.error('check_schema_integrity -> FALSE (bad keys/ not found / internal error or bad configuration')
        ###raise

    log.info('<<< leave {}()'.format(func_name))


def _ingest_data():
    pass

def _select(query):
    #TBD:  first pass validation query
    _not_stored = _not_cached = True
    if _not_cached: 
        if _not_stored:
            data = _ingest(query)
            if not data:
                return False
    return data


def validate_keys(f):
    pass
    # load keys from filename
    # open f
    # header = f.readline()     # itertools.islice
    # load keys from header
    # if keys are OK:
    #   return f ### handle for _ingest_data()
    # else


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
