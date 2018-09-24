#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

'''
DEFINISCE I SETTINGS A LIVELLO DI ENGINE, ATTUALMENTE FATTA
ECCEZIONE PER _CACHE_SIZE_ TUTTI GLI ALTRI SETTINGS
SONO STATI DEFINITI IN ds_settings.py (DATASOURCE LEVEL)
SARA' NECESSARIO SELEZIONARLI IN BASE AL CRITERIO PER CUI
SE IL SETTING SI RIFERISCE AD UNA ISTANZA DI DATASOURCE RIMANE
IN ds_settings.py ALTRIMENTI DOVRA' ESSERE SPOSTATO QUI
(IN MODO TALE CHE LA CLASSE Connection() NON POSSA MODIFICARLO...)



'''

#_MAPPERS_PATTERN_STYLE_     = 'mappers_pattern_style'
#_VERIFY_SCHEMA_INTEGRITY_   = 'verify_schema_integrity' 

_CACHE_SIZE_                = 'cache_size'

'''
CONSTANT SETTINGS
const = (_REGEX_,
         _GMATCH_, 
        )
'''


'''
DEFAULT SETTINGS
'''
defaults = {
            _CACHE_SIZE_            : 'to_be_specified',
           }

'''
CONFIGURATION SETTINGS
'''
#policy = {}

