#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

'''
KEYS SETTINGS
'''
_MAPS_          = 'mappers'
_INGEST_        = 'ingest'
_REGEX_         = 'regex'
_GMATCH_        = 'gmatch'

'''SCHEMA POLICY OPTIONS'''
_SCHEMA_DS_ROOT_ONLY_       = 'schema_ds_root_only'     # option
_SCHEMA_ALL_SUBFOLDERS_     = 'schema_all_subfolders'   # option

#_SCHEMA_DS_ROOT_ONLY_       = 0x0001
#_SCHEMA_ALL_SUBFOLDERS_     = 0x0002

'''SCHEMA POLICY DEFAULT OPTION'''
_SCHEMA_SCAN_POLICY_        = _SCHEMA_DS_ROOT_ONLY_     # Default NOT constant
'''SCHEMA POLICY KEY'''
_SCHEMA_SCAN_OPTION_        = 'schema_scan_option'      # option key const
'''option set - ad uso della f(x) di controllo):
    if configuration[schema_scan_option] in _SCHEMA_SET:
        if configuration[schema_scan_option] & _SCHEMA_DS_ROOT_ONLY_:
            policy = _SCHEMA_DS_ROOT_ONLY_
            ...
    else: bad configuration
'''
_SCHEMA_SET = {_SCHEMA_DS_ROOT_ONLY_, _SCHEMA_ALL_SUBFOLDERS_}

_ACCEPT_CONST_OVERRIDE_     = 'accept_const_override' 
_MAPPERS_PATTERN_STYLE_     = 'mappers_pattern_style'
_VERIFY_SCHEMA_INTEGRITY_   = 'verify_schema_integrity' 


'''
CONSTANT SETTINGS
'''
const = (_MAPS_, 
         _INGEST_, 
         _REGEX_, 
         _GMATCH_, 
         _ACCEPT_CONST_OVERRIDE_,

         _SCHEMA_DS_ROOT_ONLY_,
         _SCHEMA_ALL_SUBFOLDERS_,

         _SCHEMA_SCAN_OPTION_,  # option key const
        )
'''
DEFAULT SETTINGS
'''
defaults = {
            _MAPS_      : _MAPS_,
            _INGEST_    : _INGEST_,
            _REGEX_     : _REGEX_,
            _GMATCH_    : _GMATCH_,
            _VERIFY_SCHEMA_INTEGRITY_   : True,
            _SCHEMA_DS_ROOT_ONLY_       : 'schema_ds_root_only',
            _SCHEMA_ALL_SUBFOLDERS_     : 'schema_all_subfolders',
            _ACCEPT_CONST_OVERRIDE_     : False,
            _MAPPERS_PATTERN_STYLE_     : 'resource_mapper.*.json',
            _SCHEMA_SCAN_POLICY_        : _SCHEMA_DS_ROOT_ONLY_,
            _SCHEMA_SCAN_OPTION_: _SCHEMA_SCAN_POLICY_, # option key const + Default NOT constant value
           }
'''
CONFIGURATION SETTINGS
'''
policy = {}


