#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
'''
quant2018 eoddata adapter
'''
import os, errno, sys, logging
import fnmatch
import json   
import collections 
from loader import load_adapter

this = sys.modules[__name__]

this.log = None
this.resource_mapper_template = collections.OrderedDict() 
this.provider_path = None

_adapters = _connections = {}


def init(conf):
    pass


def connect(adapter, datasource):

    log = logging.getLogger(__name__) 
    try:
        if adapter not in _adapters:
            _adapters[adapter] = adapter = load_adapter('', 'modx')  # 'ohlcv_adapter'
            log.info('==> adapter loaded')
        else:
            adapter = _adapters[adapter]
            log.info('adapter already loaded!')

    except Exception as e:
        print('exception : {}'.format(e))

    if datasource not in _connections:
        ### TBD
        _connections[datasource] = datasource
        log.info('new connection established')
    else:
        log.info('connection already established!')

    return _connections[datasource]

def select(query): ### quale adapter e quale connessione?....
    pass

