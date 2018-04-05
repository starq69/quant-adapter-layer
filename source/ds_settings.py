#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

#import sys, logging

_MAPS_          = 'mappers'
_INGEST_        = 'ingest'
_REGEX_         = 'regex'
_GMATCH_        = 'gmatch'
_TIMEFRAME_     = 'timeframe'
_FORMAT_        = 'format'
_SEPARATOR_     = 'sep'

_MODEL_NAME_                = 'model_name'
_DATASOURCE_NAME_           = 'datasource_name'
_DATASOURCE_ROOT_           = 'datasource_root'
_INGEST_DEFAULT_PATH_       = 'ingest_default_path'

'''SCHEMA POLICY OPTIONS'''
_SCHEMA_DS_ROOT_ONLY_       = 'schema_ds_root_only'     # option
_SCHEMA_ALL_SUBFOLDERS_     = 'schema_all_subfolders'   # option
'''SCHEMA POLICY KEY'''
_SCHEMA_SCAN_OPTION_        = 'schema_scan_option'      # option key const
_SCHEMA_SET = {_SCHEMA_DS_ROOT_ONLY_, _SCHEMA_ALL_SUBFOLDERS_}

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
        _REGEX_,
        _GMATCH_,
        )
'''
DEFAULT SETTINGS
'''
defaults = {
            _MAPS_      : _MAPS_,
            _INGEST_    : _INGEST_,
            _REGEX_     : _REGEX_,
            _GMATCH_    : _GMATCH_,
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
            # NB : ha un impatto su :
            # merge()
            #
            _CACHE_SIZE_                : 10000,
           }
'''
CONFIGURATION SETTINGS
'''
policy = {}

