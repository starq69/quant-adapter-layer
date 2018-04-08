#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

#import sys, logging

_MAPS_          = 'mappers'
_INGEST_        = 'ingest'
#_REGEX_         = 'regex'
_FPATTERN_      = 'fpattern'   # ex _REGEX_
#_GMATCH_        = 'gmatch'
_KEYMATCH_      = 'keymatch'  # ex _GMATCH_
_TIMEFRAME_     = 'timeframe'
#_FORMAT_        = 'format'
_FFORMAT_       = 'fformat'  # ex _FORMAT_
_SEPARATOR_     = 'sep'

''' DATA KEYS '''
_MKT_           = 'MKT'
_SYM_           = 'SYM'
_TIMEFRAME_     = 'timeframe'
_TIMESTAMP_     = 'timestamp'

''' DATA KEYS ATTRIBUTES '''
#_MKT_DEEP_          = '@MKT_DEEP_'
#_SYM_DEEP_          = '@SYM_DEEP_'
#_TIMEFRAME_DEEP_    = '@timeframe_DEEP_'
#_TIMESTAMP_DEEP_    = '@timestamp_DEEP_'
#
#_MKT_IS_MANDATORY_  = '@MKT_IS_MANDATORY_'
#_MKT_IS_NODE_       = '@MKT_IS_NODE_'
#_MKT_HAS_PARENT_    = '@MKT_HAS_PARENT_'
#_MKT_PARENT_        = '@MKT_PARENT_'
#
#_SYM_IS_MANDATORY_   = '@SYM_IS_MANDATORY_'
#_SYM_IS_NODE_        = '@SYM_IS_NODE_'
#_SYM_HAS_PARENT_     = '@SYM_HAS_PARENT_'
#_SYM_PARENT_         = '@SYM_PARENT_' 
#
#_TIMEFRAME_IS_MANDATORY_   = '@TIMEFRAME_IS_MANDATORY_'
#_TIMEFRAME_IS_NODE_        = '@TIMEFRAME_IS_NODE_'
#_TIMEFRAME_HAS_PARENT_     = '@TIMEFRAME_HAS_PARENT_'
#_TIMEFRAME_PARENT_         = '@TIMEFRAME_PARENT_' 
#
#_TIMESTAMP_IS_MANDATORY_   = '@TIMESTAMP_IS_MANDATORY_'
#_TIMESTAMP_IS_NODE_        = '@TIMESTAMP_IS_NODE_'
#_TIMESTAMP_HAS_PARENT_     = '@TIMESTAMP_HAS_PARENT_'
#_TIMESTAMP_PARENT_         = '@TIMESTAMP_PARENT_' 

''' KEY ATTR OPTIONS '''
_MANDATORY_                 = 'mandatory'
_IS_NODE_                   = 'is_node'
_HAS_PARENT_                = 'has_parent'
''' KEY ATTR SET '''
_KEY_SET_ATTR_      = {_MANDATORY_, _IS_NODE_, _HAS_PARENT_}


_MKT_ATTR_          = '@MKT_ATTR_'
_SYM_ATTR_          = '@SYM_ATTR_'
_TIMEFRAME_ATTR_    = '@timeframe_ATTR_'
_TIMESTAMP_ATTR_    = '@timestamp_ATTR_'

_MODEL_NAME_                = 'model_name'
_DATASOURCE_NAME_           = 'datasource_name'
_DATASOURCE_ROOT_           = 'datasource_root'
_INGEST_DEFAULT_PATH_       = 'ingest_default_path'

'''SCHEMA POLICY OPTIONS'''
_SCHEMA_DS_ROOT_ONLY_       = 'schema_ds_root_only'     # option
_SCHEMA_ALL_SUBFOLDERS_     = 'schema_all_subfolders'   # option
'''SCHEMA POLICY KEY'''
_SCHEMA_SCAN_OPTION_        = 'schema_scan_option'      # option key const
_SCHEMA_SET_ = {_SCHEMA_DS_ROOT_ONLY_, _SCHEMA_ALL_SUBFOLDERS_}

_ACCEPT_CONST_OVERRIDE_     = 'accept_const_override' 
_MAPPERS_PATTERN_STYLE_     = 'mappers_pattern_style'
_VERIFY_SCHEMA_INTEGRITY_   = 'verify_schema_integrity' 
_INGEST_DEFAULT_FILE_PATTERN_ = 'ingest_default_file_pattern'
_INGEST_FILE_PATTERNS_      = 'ingest_file_patterns'

_CACHE_SIZE_                = 'cache-size'

'''
CONSTANT SETTINGS
'''
const = (_ACCEPT_CONST_OVERRIDE_,
         _SCHEMA_DS_ROOT_ONLY_,
         _SCHEMA_ALL_SUBFOLDERS_,
         _CACHE_SIZE_,
        _MAPS_, 
        _INGEST_,
#        _REGEX_,
#        _GMATCH_,
        _FPATTERN_,
        _KEYMATCH_,

#        _MKT_DEEP_,
#        _SYM_DEEP_,
#        _TIMEFRAME_DEEP_,
#        _TIMESTAMP_DEEP_,

#        _MKT_IS_MANDATORY_,
#        _MKT_IS_NODE_,
#        _MKT_HAS_PARENT_,
#        _MKT_PARENT_,
#
#        _SYM_IS_MANDATORY_,
#        _SYM_IS_NODE_,
#        _SYM_HAS_PARENT_,
#        _SYM_PARENT_,
#
#        _TIMEFRAME_IS_MANDATORY_,
#        _TIMEFRAME_IS_NODE_,
#        _TIMEFRAME_HAS_PARENT_,
#        _TIMEFRAME_PARENT_,
#
#        _TIMESTAMP_IS_MANDATORY_,
#        _TIMESTAMP_IS_NODE_,
#        _TIMESTAMP_HAS_PARENT_,
#        _TIMESTAMP_PARENT_,

        _MANDATORY_,
        _IS_NODE_,
        _HAS_PARENT_,
        _KEY_SET_ATTR_
        )
'''
DEFAULT SETTINGS
'''
defaults = {
            _MAPS_      : _MAPS_,
            _INGEST_    : _INGEST_,
#            _REGEX_     : _REGEX_,
#            _GMATCH_    : _GMATCH_,
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
            _MAPPERS_PATTERN_STYLE_     : 'resource_mapper.*.json',
            _SCHEMA_SCAN_OPTION_        : _SCHEMA_DS_ROOT_ONLY_, 
            
            _INGEST_DEFAULT_FILE_PATTERN_ : '*.txt', # potrebbe essere una lista...
            _INGEST_FILE_PATTERNS_        : ['*.txt', '*.dat'], #...segue sulla get_file_items()
            _CACHE_SIZE_                : 10000,

#            _MKT_DEEP_           : 0,
#            _SYM_DEEP_           : 1,
#            _TIMEFRAME_DEEP_     : 2,
#            _TIMESTAMP_DEEP_     : 3,
#
#            _MKT_IS_MANDATORY_              : False,
#            _SYM_IS_MANDATORY_          : True,
#            _TIMEFRAME_IS_MANDATORY_    : True,
#            _TIMESTAMP_IS_MANDATORY_    : True,
#
#            _MKT_IS_NODE_               : True,
#            _SYM_IS_NODE_               : True,
#            _TIMEFRAME_IS_NODE_             : False,
#            _TIMESTAMP_IS_NODE_         : True,
#
#            _MKT_HAS_PARENT_                : False,
#            _SYM_HAS_PARENT_            : False,
#            _TIMEFRAME_HAS_PARENT_      : False,
#            _TIMESTAMP_HAS_PARENT_      : True,

            _MKT_ATTR_              : {_IS_NODE_},
            _SYM_ATTR_              : {_MANDATORY_, _IS_NODE_},
            _TIMEFRAME_ATTR_        : {_MANDATORY_},
            _TIMESTAMP_ATTR_        : {_MANDATORY_, _HAS_PARENT_},
           }
'''
CONFIGURATION SETTINGS
'''
policy = {}

