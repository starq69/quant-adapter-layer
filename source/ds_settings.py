#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

#import sys, logging

_MAPS_          = 'mappers'
_TABLES_        = 'tables'
_INGEST_        = 'ingest'
_FPATTERN_      = 'fpattern'   # ex _REGEX_
_KEYMATCH_      = 'keymatch'  # ex _GMATCH_
_TIMEFRAME_     = 'timeframe'
_FFORMAT_       = 'fformat'  # ex _FORMAT_
_SEPARATOR_     = 'sep'

''' DATA KEYS '''
_MKT_           = '@MKT'
_SYM_           = '@SYM'
_TIMEFRAME_     = '@timeframe'
_TIMESTAMP_     = '@timestamp'



''' KEY ATTR OPTIONS '''
#_MANDATORY_                 = 'mandatory' ### se le keys sono tutte mandatory questo NON serve
_IS_NODE_                   = 'is_node'
_HAS_PARENT_                = 'has_parent'

#'''KEY PARENT NODE OPTIONS '''
#_PARENT_REQUIRED_          = 'key_parent_node_required'
#_PARENT_NOT_REQUIRED_      = 'key_parent_node_not_required'
#_PARENT_OPTIONAL_          = 'key_parent_node_optional'
#_KEY_PARENT_NODE_OPTION_   = 'key_parent_node_option'
#_KEY_PARENT_NODE_SET_ = {_REQUIRED_, _NOT_REQUIRED_, _OPTIONAL_}



''' KEY ATTR SET '''
_KEY_SET_ATTR_      = {_IS_NODE_, _HAS_PARENT_}
#_KEY_SET_ATTR_      = {_MANDATORY_, _IS_NODE_, _HAS_PARENT_}
#_KEY_SET_ATTR_      = {_MANDATORY_, _IS_NODE_, _KEY_PARENT_NODE_OPTION_}

''' KEY ATTR SETTINGS '''
_MKT_ATTR_          = '@MKT_ATTR_'
_SYM_ATTR_          = '@SYM_ATTR_'
_TIMEFRAME_ATTR_    = '@timeframe_ATTR_'
_TIMESTAMP_ATTR_    = '@timestamp_ATTR_'

_SYM_PARENT_        = '@SYM_PARENT_'
_TIMESTAMP_PARENT_  = '@timestamp_PARENT_'



_MODEL_NAME_                = 'model_name'
_DATASOURCE_NAME_           = 'datasource_name'
_DATASOURCE_ROOT_           = 'datasource_root'
_INGEST_DEFAULT_PATH_       = 'ingest_default_path'

'''SCHEMA POLICY OPTIONS'''
_SCHEMA_DS_ROOT_ONLY_       = 'schema_ds_root_only'     # option
_SCHEMA_ALL_SUBFOLDERS_     = 'schema_all_subfolders'   # option
'''SCHEMA POLICY SET'''
_SCHEMA_SET_ = {_SCHEMA_DS_ROOT_ONLY_, _SCHEMA_ALL_SUBFOLDERS_}
'''SCHEMA POLICY SETTING'''
_SCHEMA_SCAN_OPTION_        = 'schema_scan_option'      # option key const

_ACCEPT_CONST_OVERRIDE_     = 'accept_const_override' 
_VERIFY_SCHEMA_INTEGRITY_   = 'verify_schema_integrity' 
_INGEST_DEFAULT_FILE_PATTERN_ = 'ingest_default_file_pattern'
_INGEST_FILE_PATTERNS_      = 'ingest_file_patterns'

_MAPPERS_PATTERN_STYLE_     = 'mappers_pattern_style'

#issue load_schema improved
_DATASET_MAP_FILES_         = 'dataset_map_files'
_INGEST_MAP_FILES_          = 'ingest_map_files'    # ex _MAPPERS_PATTERN_STYLE_

_CACHE_SIZE_                = 'cache_size'

'''
CONSTANT SETTINGS
'''
const = (_ACCEPT_CONST_OVERRIDE_,
         _SCHEMA_DS_ROOT_ONLY_,
         _SCHEMA_ALL_SUBFOLDERS_,
         _CACHE_SIZE_,
         _MAPS_, 
         _TABLES_,
         _INGEST_,
         _FPATTERN_,
         _KEYMATCH_,
 
         #_MANDATORY_,
         _IS_NODE_,
         _HAS_PARENT_,
         _KEY_SET_ATTR_,
         _SYM_PARENT_,
         _TIMESTAMP_PARENT_,
        )

# NEW app keys
app = frozenset([_DATASOURCE_NAME_])
'''
DEFAULT SETTINGS
'''
defaults = {
            _MAPS_      : _MAPS_,
            _TABLES_    : _TABLES_,
            _INGEST_    : _INGEST_,
            _FPATTERN_  : _FPATTERN_, # ex regex
            _KEYMATCH_  : _KEYMATCH_, # ex gmatch
            _MODEL_NAME_                : 'undef',
            _DATASOURCE_NAME_           : 'undef',
            _DATASOURCE_ROOT_           : 'undef',
            _INGEST_DEFAULT_PATH_       : 'undef',
            _VERIFY_SCHEMA_INTEGRITY_   : True,
            _SCHEMA_DS_ROOT_ONLY_       : 'schema_ds_root_only',
            _SCHEMA_ALL_SUBFOLDERS_     : 'schema_all_subfolders',
            _ACCEPT_CONST_OVERRIDE_     : False,

            _MAPPERS_PATTERN_STYLE_     : 'resource_mapper.*.json', ## OLD
            _INGEST_MAP_FILES_          : 'ingest.mapper.*.json',   ## NEW
            _DATASET_MAP_FILES_         : 'dataset.definition.*.json',     ## NEW

            _SCHEMA_SCAN_OPTION_        : _SCHEMA_DS_ROOT_ONLY_, 
            
            _INGEST_DEFAULT_FILE_PATTERN_   : '*.txt', # potrebbe essere una lista...
            _INGEST_FILE_PATTERNS_          : ['*.txt', '*.dat'], #...segue sulla get_file_items()
            _CACHE_SIZE_                    : 10000,

            #_MKT_ATTR_              : {_IS_NODE_, _KEY_PARENT_NODE_SET_(_NOT_REQUIRED_)},

            _MKT_ATTR_              : {_IS_NODE_},
            _SYM_ATTR_              : {_IS_NODE_, _HAS_PARENT_},
            _TIMEFRAME_ATTR_        : {},   # GLOBAL/DEFAULT ?
            _TIMESTAMP_ATTR_        : {_HAS_PARENT_},

            _SYM_PARENT_            : _MKT_,
            #_TIMESTAMP_PARENT_      : _SYM_,
            _TIMESTAMP_PARENT_      : _MKT_, #TEST
           }
#'''
#CONFIGURATION SETTINGS
#'''
#policy = {}

