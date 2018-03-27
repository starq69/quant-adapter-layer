#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging
from loader import load_adapter
import conventions

_adapters = {}

class Connection(object):

    def __init__(self, config, adapter, datasource):

        self.log        = logging.getLogger(__name__)
        self.config     = config
        self.adapter    = adapter
        self.datasource = datasource

        if self.adapter not in _adapters: 
            try:
                self.adapter = _adapters[self.adapter] = load_adapter('', 'modx') 
                #import modx as adapter
                #self.adapter.init(self.config) ..ora nella __enter__
                self.log.info('==> adapter <{}> loaded'.format(adapter))
            except Exception as e:
                self.log.error('fail to load adapter {} : '.format(adapter, e))
        else:
            self.adapter = _adapters[self.adapter]                         


    def __enter__(self):

        adp = self.adapter
        ds  = self.datasource

        adp.init(self.config)
        '''
        if adp not in _adapters:
           # try:
            #adp = _adapters[adp] = load_adapter('', 'modx')  
            import modx as adapter
            adapter.init(self.config)
            self.log.info('==> adapter <{}> loaded'.format(adp))
           # except Exception as e:
           #     self.log.error('__enter__ exception : {}'.format(e)) 
        else:
            self.adapter = _adapters[self.adapter]

        adp.connect(ds)
            adp.load_schema(ds)
            adp.load_index (ds)
            adp.load_cache (ds)
        '''
        return self

    def __exit__(self, e_typ, e_val, trcbak):
        self.log.info('release resource')
        pass

    def select(self, query):
        self.log.info('select ok')

