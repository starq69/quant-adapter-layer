#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, os, logging
from loader import load_adapter
import conventions

__all__ = ['Connection']

_models = {}

class Connection(object):

    def __init__(self, config, model, datasource):

        self.log        = logging.getLogger(__name__)
        self.config     = config
        self.model      = model.strip()
        self.datasource = datasource

        if model not in _models: 
            try:
                _models[model] = load_adapter('TBD', model) 
                self.log.info('==> model <{}> loaded'.format(model))

            except Exception as e:
                self.log.error('fail to load model {} : '.format(model, e))


    def __enter__(self):

        model   = _models[self.model]
        ds      = self.datasource

        model.init(self.config)
        '''
        if adp not in _models:
           # try:
            #adp = _models[adp] = load_model('', 'modx')  
            import modx as model
            model.init(self.config)
            self.log.info('==> model <{}> loaded'.format(adp))
           # except Exception as e:
           #     self.log.error('__enter__ exception : {}'.format(e)) 
        else:
            self.model = _models[self.model]

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

