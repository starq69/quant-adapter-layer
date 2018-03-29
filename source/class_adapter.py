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

        model.init(self.config) ### PROPOSED NEW NAME : model.merge(self.config)

        model.registerConnection(ds)
        model.load_schema(ds)   ### now is : def load_schema(path, scan_policy=None):
        model.load_index (ds)
        model.load_cache (ds)

        return self

    def __exit__(self, e_typ, e_val, trcbak):
        self.log.info('release resource')
        pass

    def select(self, query):
        self.log.info('select ok')

