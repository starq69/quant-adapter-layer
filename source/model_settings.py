#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-


'''
MODEL KEYS SETTINGS
'''
_MAPS_          = 'mappers'
_INGEST_        = 'ingest'
_REGEX_         = 'regex'
_GMATCH_        = 'gmatch'


_MAPPERS_PATTERN_STYLE_     = 'mappers_pattern_style'
_VERIFY_SCHEMA_INTEGRITY_   = 'verify_schema_integrity' 

_CACHE_SIZE_                = 'cache_size'

'''
CONSTANT SETTINGS
'''
const = (_MAPS_, 
         _INGEST_, 
         _REGEX_, 
         _GMATCH_, 
#         _CACHE_SIZE_,
        )
'''
DEFAULT SETTINGS
'''
defaults = {
            _MAPS_      : _MAPS_,
            _INGEST_    : _INGEST_,
            _REGEX_     : _REGEX_,
            _GMATCH_    : _GMATCH_,
            _CACHE_SIZE_            : '100M',
           }
'''
CONFIGURATION SETTINGS
'''
#policy = {}

