#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
proposed new name: ID (import policy as ID)
                        
                       import constants as ID


N.B
definisce le policy dell'adapter

eventuali policy di connessione (supplementary policy)
andrebbero definite su un altro modulo il cui nome è def. in conf
che viene importato e passato al costruttore di Connection

'''

defaults = {
        'foo_policy': True,
        'bar_policy': True,
        }

'''
STORAGE SCHEMA POLICY
'''
_SCHEMA_DS_ROOT_ONLY_      = 1
_SCHEMA_ALL_SUBFOLDERS_    = 2

'''
SCHEMA KEYS
'''
_maps_          = 'mappers'
_ingest_        = 'ingest' 

_REGEX_         = 'regex' 
_GMATCH_        = 'gmatch' 


_verify_schema_integrity_   = True


'''
_MAPS_ex_       = encode('mappers')

def encode(s):
    number = 0
    for e in [ord(c) for c in s]:
        number = (number * 0x110000) + e
    return number


def key(number):
    l = []
    while(number != 0):
        l.append(chr(number % 0x110000))
        number = number // 0x110000
    return ''.join(reversed(l))
'''
